from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import pandas as pd
from pydataxm.pydatasimem import ReadSIMEM


logger = logging.getLogger(__name__)

CASCADA = ["TXF", "TXR", "TX8", "TX7", "TX6", "TX5", "TX4", "TX3", "TX2", "TX1"]
VERSION_RANK = {version: index + 1 for index, version in enumerate(CASCADA)}
TIPOS_VALIDOS = ["Hidraulica", "Termica", "Solar", "Eolica", "Cogenerador"]
KWH_A_MWH = 1_000
CHUNK_DAYS = 30


def _build_date_chunks(start_date: date, end_date: date, chunk_days: int = CHUNK_DAYS) -> list[tuple[str, str]]:
    chunks: list[tuple[str, str]] = []
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        chunks.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end + timedelta(days=1)
    return chunks


class SimemClient:
    def _fetch_dataset(self, dataset_id: str, start_date: date, end_date: date) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for chunk_start, chunk_end in _build_date_chunks(start_date, end_date):
            logger.info("Downloading SIMEM dataset %s: %s -> %s", dataset_id, chunk_start, chunk_end)
            frame = ReadSIMEM(dataset_id, chunk_start, chunk_end).main(filter=False)
            if frame is None or frame.empty:
                continue
            frames.append(frame)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def fetch_context_range(
        self,
        *,
        start_date: date,
        end_date: date,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        price_raw = self._fetch_dataset("EC6945", start_date, end_date)
        demand_raw = self._fetch_dataset("D55202", start_date, end_date)
        catalog_raw = self._fetch_dataset("E17D25", start_date, end_date)
        generation_raw = self._fetch_dataset("8E7F3C", start_date, end_date)

        if price_raw.empty or demand_raw.empty or generation_raw.empty:
            raise RuntimeError("SIMEM returned insufficient data for the requested range")

        raw_payload = {
            "spot_price": price_raw.copy(),
            "demand": demand_raw.copy(),
            "plants": catalog_raw.copy(),
            "generation": generation_raw.copy(),
        }

        price_raw = price_raw[price_raw["CodigoVariable"] == "PB_Nal"].copy()
        price_raw["Date"] = pd.to_datetime(price_raw["FechaHora"]).dt.floor("h")
        price_raw["Valor"] = pd.to_numeric(price_raw["Valor"], errors="coerce")
        price_raw = price_raw[price_raw["Version"].isin(CASCADA)].copy()
        price_raw["vrank"] = price_raw["Version"].map(VERSION_RANK).astype("int8")
        price_raw = price_raw.sort_values(["Date", "vrank"]).drop_duplicates("Date", keep="first")
        price_raw["fecha"] = pd.to_datetime(price_raw["Date"].dt.date)
        precio = (
            price_raw.groupby("fecha")["Valor"]
            .mean()
            .reset_index()
            .rename(columns={"Valor": "Precio_mean"})
        )

        demand_raw["Date"] = pd.to_datetime(demand_raw["FechaHora"]).dt.floor("h")
        demand_raw["Valor"] = pd.to_numeric(demand_raw["Valor"], errors="coerce")
        demand_raw = demand_raw[demand_raw["Version"].isin(CASCADA)].copy()
        demand_raw["vrank"] = demand_raw["Version"].map(VERSION_RANK).astype("int8")
        demand_raw = demand_raw.sort_values(["Date", "CodigoSICAgente", "TipoMercado", "vrank"]).drop_duplicates(
            ["Date", "CodigoSICAgente", "TipoMercado"],
            keep="first",
        )
        demand_raw["fecha"] = pd.to_datetime(demand_raw["Date"].dt.date)
        demanda = (
            demand_raw.groupby("fecha")["Valor"]
            .sum()
            .reset_index()
            .assign(Demanda_dia=lambda frame: frame["Valor"] / KWH_A_MWH)[["fecha", "Demanda_dia"]]
        )

        catalog = pd.DataFrame(columns=["CodigoPlanta", "TipoGeneracion"])
        if not catalog_raw.empty:
            catalog = (
                catalog_raw[["CodigoPlanta", "TipoGeneracion"]]
                .pipe(lambda frame: frame[frame["TipoGeneracion"].isin(TIPOS_VALIDOS)])
                .drop_duplicates("CodigoPlanta")
                .reset_index(drop=True)
            )

        generation_raw["Date"] = pd.to_datetime(generation_raw["FechaHora"]).dt.floor("h")
        generation_raw["Valor"] = pd.to_numeric(generation_raw["Valor"], errors="coerce")
        generation_raw = generation_raw[generation_raw["CodigoVariable"] == "GIdealNal"].copy()
        if not catalog.empty:
            generation_raw = generation_raw.merge(catalog, on="CodigoPlanta", how="left")
        generation_raw = generation_raw.dropna(subset=["TipoGeneracion"])
        generation_raw = generation_raw[generation_raw["TipoGeneracion"].isin(TIPOS_VALIDOS)].copy()
        generation_raw = generation_raw[generation_raw["Version"].isin(CASCADA)].copy()
        generation_raw["vrank"] = generation_raw["Version"].map(VERSION_RANK).astype("int8")
        generation_raw = generation_raw.sort_values(["Date", "CodigoPlanta", "vrank"]).drop_duplicates(
            ["Date", "CodigoPlanta"],
            keep="first",
        )
        generation_raw["Valor_MWh"] = generation_raw["Valor"] / KWH_A_MWH
        generation_raw["fecha"] = pd.to_datetime(generation_raw["Date"].dt.date)
        gen = (
            generation_raw.groupby(["fecha", "TipoGeneracion"])["Valor_MWh"]
            .sum()
            .reset_index()
            .pivot_table(
                index="fecha",
                columns="TipoGeneracion",
                values="Valor_MWh",
                aggfunc="sum",
                fill_value=0,
                observed=True,
            )
            .reset_index()
        )
        gen.columns.name = None
        for generation_type in TIPOS_VALIDOS:
            if generation_type not in gen.columns:
                gen[generation_type] = 0.0
        gen["fecha"] = pd.to_datetime(gen["fecha"])

        daily = precio.merge(demanda, on="fecha", how="left").merge(gen, on="fecha", how="left")
        return daily.sort_values("fecha").reset_index(drop=True), raw_payload

    def fetch_actual_prices_range(self, *, start_date: date, end_date: date) -> dict[str, float]:
        raw = self._fetch_dataset("EC6945", start_date, end_date)
        if raw.empty:
            return {}
        raw = raw[raw["CodigoVariable"] == "PB_Nal"].copy()
        raw["Date"] = pd.to_datetime(raw["FechaHora"]).dt.floor("h")
        raw["Valor"] = pd.to_numeric(raw["Valor"], errors="coerce")
        raw = raw[raw["Version"].isin(CASCADA)].copy()
        raw["vrank"] = raw["Version"].map(VERSION_RANK).astype("int8")
        raw = raw.sort_values(["Date", "vrank"]).drop_duplicates("Date", keep="first")
        raw["fecha"] = raw["Date"].dt.date.astype(str)
        return raw.groupby("fecha")["Valor"].mean().to_dict()
