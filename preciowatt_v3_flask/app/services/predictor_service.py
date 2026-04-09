from __future__ import annotations

import logging
import math
import threading
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from ..repositories.dynamodb_feature_repository import DailyFeatureRepository
from ..repositories.s3_asset_repository import S3AssetRepository
from ..repositories.sync_status_repository import SyncStatusRepository
from ..runtime_config import RuntimeConfig
from .simem_client import TIPOS_VALIDOS


def _nan_safe(obj):
    if isinstance(obj, dict):
        return {key: _nan_safe(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_nan_safe(value) for value in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, (np.floating,)):
        value = float(obj)
        return None if (math.isnan(value) or math.isinf(value)) else value
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    return obj


def _round_metric(value: float | None) -> float | None:
    if value is None or math.isnan(value) or math.isinf(value):
        return None
    return round(float(value), 2)


def _compute_regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float | None]:
    if y_true.size == 0:
        return {"mape": None, "mae": None, "rmse": None, "r2": None}

    abs_error = np.abs(y_true - y_pred)
    mae = float(np.mean(abs_error))
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

    non_zero_mask = y_true != 0
    if np.any(non_zero_mask):
        mape = float(np.mean((abs_error[non_zero_mask] / np.abs(y_true[non_zero_mask])) * 100))
    else:
        mape = None

    ss_res = float(np.sum((y_true - y_pred) ** 2))
    ss_tot = float(np.sum((y_true - np.mean(y_true)) ** 2))
    r2 = None if ss_tot == 0 else float(1 - (ss_res / ss_tot))

    return {
        "mape": _round_metric(mape),
        "mae": _round_metric(mae),
        "rmse": _round_metric(rmse),
        "r2": _round_metric(r2),
    }


def _fallback_metric(value: object, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        value_f = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(value_f) or math.isinf(value_f):
        return default
    return _round_metric(value_f)


def recalcular_features(df: pd.DataFrame, feats: list[str]) -> pd.DataFrame:
    d = df.copy()
    features = set(feats)

    for generation_type in TIPOS_VALIDOS:
        if generation_type not in d.columns:
            d[generation_type] = 0.0

    if "embalse_pct_nacional" not in d.columns:
        d["embalse_pct_nacional"] = 0.0

    d["embalse_pct_nacional"] = pd.to_numeric(d["embalse_pct_nacional"], errors="coerce").ffill().bfill().fillna(0.0)

    if all(column in d.columns for column in TIPOS_VALIDOS):
        d["GenTotal"] = d[TIPOS_VALIDOS].sum(axis=1).clip(lower=1.0)
        if "share_hidraulica" in features:
            d["share_hidraulica"] = d["Hidraulica"] / d["GenTotal"]
        if "share_termica" in features:
            d["share_termica"] = d["Termica"] / d["GenTotal"]
        if "ratio_termica_hidraulica" in features:
            d["ratio_termica_hidraulica"] = d["Termica"] / d["Hidraulica"].clip(lower=1.0)

    price_mean = d["Precio_mean"]
    if "precio_media_7d" in features:
        d["precio_media_7d"] = price_mean.rolling(7, min_periods=1).mean()
    if "precio_std_14d" in features:
        d["precio_std_14d"] = price_mean.rolling(14, min_periods=2).std()
    d["precio_lag_1d"] = price_mean.shift(1)
    d["precio_lag_1d_log"] = np.log1p(d["precio_lag_1d"].clip(lower=0))
    if "precio_lag_2d" in features:
        d["precio_lag_2d"] = price_mean.shift(2)
    if "precio_lag_3d" in features:
        d["precio_lag_3d"] = price_mean.shift(3)
    if "precio_cambio_3d" in features:
        d["precio_cambio_3d"] = ((price_mean - price_mean.shift(3)) / price_mean.shift(3).clip(lower=1.0)) * 100
    if "precio_cambio_7d" in features:
        d["precio_cambio_7d"] = ((price_mean - price_mean.shift(7)) / price_mean.shift(7).clip(lower=1.0)) * 100
    if "embalse_cambio_7d" in features:
        d["embalse_cambio_7d"] = d["embalse_pct_nacional"].diff(7)
    if "hidraulica_cambio_7d" in features:
        hydro_mean = d["Hidraulica"].rolling(7, min_periods=1).mean()
        d["hidraulica_cambio_7d"] = hydro_mean - hydro_mean.shift(7)
    if "ratio_cambio_termica" in features:
        thermal_mean = d["Termica"].rolling(7, min_periods=1).mean()
        d["ratio_cambio_termica"] = ((thermal_mean - thermal_mean.shift(7)) / thermal_mean.shift(7).clip(lower=1.0)) * 100
    if "precio_sobre_media_30d" in features:
        mean_30 = price_mean.rolling(30, min_periods=7).mean().clip(lower=1.0)
        d["precio_sobre_media_30d"] = (price_mean / mean_30) - 1
    if "ratio_termica_cambio_3d" in features:
        if "ratio_termica_hidraulica" not in d.columns:
            d["ratio_termica_hidraulica"] = d["Termica"] / d["Hidraulica"].clip(lower=1.0)
        d["ratio_termica_cambio_3d"] = d["ratio_termica_hidraulica"] - d["ratio_termica_hidraulica"].shift(3)
    if "dias_bajando_consecutivos" in features:
        if "precio_sobre_media_30d" not in d.columns:
            mean_30 = price_mean.rolling(30, min_periods=7).mean().clip(lower=1.0)
            d["precio_sobre_media_30d"] = (price_mean / mean_30) - 1
        bajando = (d["precio_sobre_media_30d"].diff() < 0).astype(int)
        group = (bajando != bajando.shift()).cumsum()
        d["dias_bajando_consecutivos"] = bajando.groupby(group).cumsum() * bajando
    if "termica_media_7d" in features:
        d["termica_media_7d"] = d["Termica"].rolling(7, min_periods=1).mean()
    if "hidraulica_media_7d" in features:
        d["hidraulica_media_7d"] = d["Hidraulica"].rolling(7, min_periods=1).mean()
    if "presion_termica_14d" in features:
        if "termica_media_7d" not in d.columns:
            d["termica_media_7d"] = d["Termica"].rolling(7, min_periods=1).mean()
        if "hidraulica_media_7d" not in d.columns:
            d["hidraulica_media_7d"] = d["Hidraulica"].rolling(7, min_periods=1).mean()
        d["presion_termica_14d"] = d["termica_media_7d"] / d["hidraulica_media_7d"].clip(lower=1.0)
    if "demanda_lag_7d" in features:
        d["demanda_lag_7d"] = d["Demanda_dia"].shift(7)
    if "deficit_hidraulico" in features:
        if "hidraulica_media_7d" not in d.columns:
            d["hidraulica_media_7d"] = d["Hidraulica"].rolling(7, min_periods=1).mean()
        if "share_hidraulica" not in d.columns:
            d["GenTotal"] = d[TIPOS_VALIDOS].sum(axis=1).clip(lower=1.0)
            d["share_hidraulica"] = d["Hidraulica"] / d["GenTotal"]
        gen_7d = d["GenTotal"].rolling(7, min_periods=1).mean().clip(lower=1.0)
        d["deficit_hidraulico"] = (d["hidraulica_media_7d"] / gen_7d) - d["share_hidraulica"]
    if "embalse_tendencia_7d" in features:
        d["embalse_tendencia_7d"] = d["embalse_pct_nacional"] - d["embalse_pct_nacional"].rolling(7, min_periods=1).mean()
    if "es_fin_semana" in features and "fecha" in d.columns:
        d["es_fin_semana"] = (pd.to_datetime(d["fecha"]).dt.dayofweek >= 5).astype("int8")

    return d


log = logging.getLogger("predictor")


class PredictorService:
    def __init__(self, runtime_config: RuntimeConfig):
        self.runtime_config = runtime_config
        self._lock = threading.Lock()
        self.error_carga = None
        self.listo = False

        self.s3_repository = S3AssetRepository(
            region_name=runtime_config.aws_region,
            bucket_name=runtime_config.bucket_name,
        )
        self.feature_repository = DailyFeatureRepository(
            region_name=runtime_config.aws_region,
            table_name=runtime_config.daily_feature_table_name,
        )
        self.sync_status_repository = SyncStatusRepository(
            region_name=runtime_config.aws_region,
            table_name=runtime_config.sync_status_table_name,
        )

        try:
            self._ensure_bootstrap_assets_in_s3()
            self.rf = self.s3_repository.load_joblib_object(runtime_config.model_object_key)
            self.config = self.s3_repository.load_joblib_object(runtime_config.config_object_key)
            self.feats = self.config["features"]
            self.umbrales = self.config["umbrales"]
            self.listo = True
            log.info("Modelo y config cargados desde S3")
        except Exception as exc:
            self.error_carga = str(exc).strip() or f"{type(exc).__name__}: {exc!r}"
            self.listo = False
            log.exception("Error cargando PredictorService")

    def _ensure_bootstrap_assets_in_s3(self) -> None:
        if not self.runtime_config.bootstrap_from_local_assets:
            return
        self.s3_repository.ensure_object(
            key=self.runtime_config.model_object_key,
            local_path=self.runtime_config.model_local_path,
        )
        self.s3_repository.ensure_object(
            key=self.runtime_config.config_object_key,
            local_path=self.runtime_config.config_local_path,
        )

    def _build_status_summary(self) -> dict[str, object]:
        latest = self.feature_repository.get_latest_date()
        backfill = self.sync_status_repository.get_record("backfill") or {}
        daily = self.sync_status_repository.get_record("daily") or {}
        last_sync_at = daily.get("last_success_at") or backfill.get("last_success_at")
        if daily.get("processed_start") and daily.get("processed_end"):
            last_sync_range = f"{daily.get('processed_start')}:{daily.get('processed_end')}"
        elif backfill.get("processed_start") and backfill.get("processed_end"):
            last_sync_range = f"{backfill.get('processed_start')}:{backfill.get('processed_end')}"
        else:
            last_sync_range = None
        last_sync_mode = daily.get("sync_mode") or backfill.get("sync_mode")
        sync_ready = (
            latest is not None
            and backfill.get("status") == "completed"
            and latest >= (date.today() - timedelta(days=self.runtime_config.daily_sync_lookback_days))
        )
        return {
            "latest": latest,
            "backfill_status": backfill.get("status", "not_started"),
            "daily_sync_status": daily.get("status", "not_started"),
            "last_sync_at": last_sync_at,
            "last_sync_range": last_sync_range,
            "last_sync_mode": last_sync_mode,
            "last_synced_date": latest.isoformat() if latest else None,
            "sync_ready_for_prediction": sync_ready,
            "last_error": daily.get("last_error") or backfill.get("last_error"),
        }

    def _build_prediction_context(self, fecha_ini_pred: date, fecha_fin_pred: date) -> pd.DataFrame:
        latest = self.feature_repository.get_latest_date()
        context_end = min(fecha_fin_pred, latest) if latest else fecha_fin_pred
        required_start = fecha_ini_pred - timedelta(days=self.runtime_config.lookback_days)
        context = self.feature_repository.get_rows_in_range(required_start, context_end)
        if context.empty:
            raise RuntimeError("No hay contexto diario disponible en DynamoDB para generar la prediccion")
        return context.sort_values("fecha").reset_index(drop=True)

    def _build_execution_metrics(self, df_pred: pd.DataFrame) -> dict[str, object] | None:
        comparable = df_pred[df_pred["Precio_Real"].notna()].copy()
        if comparable.empty:
            return None

        y_true = comparable["Precio_Real"].astype(float).to_numpy()
        y_pred = comparable["Precio_Pred"].astype(float).to_numpy()
        y_naive = comparable["Precio_Naive"].astype(float).to_numpy()

        return {
            "naive": _compute_regression_metrics(y_true, y_naive),
            "hibrido": _compute_regression_metrics(y_true, y_pred),
            "dias_con_real": int(comparable.shape[0]),
        }

    def _build_reference_metrics(self) -> dict[str, object]:
        return {
            "naive": {
                "mape": _fallback_metric(self.config.get("mape_naive"), 13.74),
                "mae": _fallback_metric(self.config.get("mae_naive"), 25.64),
                "rmse": _fallback_metric(self.config.get("rmse_naive")),
                "r2": _fallback_metric(self.config.get("r2_naive"), 0.80),
            },
            "hibrido": {
                "mape": _fallback_metric(
                    self.config.get("mape_hib_30d", self.config.get("mape_hib")),
                    9.54,
                ),
                "mae": _fallback_metric(self.config.get("mae_hib_30d", self.config.get("mae_hib")), 20.60),
                "rmse": _fallback_metric(self.config.get("rmse_hib_30d", self.config.get("rmse_hib"))),
                "r2": _fallback_metric(self.config.get("r2_hib_30d", self.config.get("r2_hib")), 0.87),
            },
            "dias_con_real": 0,
            "source": "reference",
        }

    def _predecir_loop(self, df_ext: pd.DataFrame, fecha_ini_pred: date, dias: int, ultima_hist: date) -> pd.DataFrame:
        actual_rows_by_date = (
            df_ext.assign(fecha_key=lambda frame: pd.to_datetime(frame["fecha"]).dt.strftime("%Y-%m-%d"))
            .drop_duplicates("fecha_key", keep="last")
            .set_index("fecha_key")
        )
        precio_real_map = (
            df_ext[df_ext["Precio_mean"].notna()]
            .assign(fecha_key=lambda frame: pd.to_datetime(frame["fecha"]).dt.strftime("%Y-%m-%d"))
            .drop_duplicates("fecha_key", keep="last")
            .set_index("fecha_key")["Precio_mean"]
            .to_dict()
        )
        predicciones = []
        df_pred_ctx = df_ext[df_ext["fecha"] < pd.Timestamp(fecha_ini_pred)].copy()

        for i in range(dias):
            fecha_pred = fecha_ini_pred + timedelta(days=i)
            anio_pred = fecha_pred.year
            ts_pred = pd.Timestamp(fecha_pred)

            df_pred_ctx = recalcular_features(df_pred_ctx, self.feats)
            fila = df_pred_ctx.tail(1).copy()

            y_naive = float(df_pred_ctx["Precio_mean"].iloc[-1])
            lag_log = float(np.log1p(max(y_naive, 0.01)))
            std_14d = (
                float(fila["precio_std_14d"].values[0])
                if "precio_std_14d" in fila.columns and not pd.isna(fila["precio_std_14d"].values[0])
                else float("nan")
            )
            dias_bajando = (
                int(fila["dias_bajando_consecutivos"].values[0])
                if "dias_bajando_consecutivos" in fila.columns and not pd.isna(fila["dias_bajando_consecutivos"].values[0])
                else 0
            )

            faltantes = [feature for feature in self.feats if feature not in fila.columns or pd.isna(fila[feature].values[0])]
            if faltantes:
                y_rf, y_final, decision = float("nan"), y_naive, "naive_forzado"
            else:
                x_pred = fila[self.feats].values.astype(np.float64)
                residuo_pred = float(self.rf.predict(x_pred)[0])
                y_rf = float(np.expm1(lag_log + residuo_pred))
                umbral = self.umbrales.get(anio_pred, self.umbrales.get(max(self.umbrales.keys()), 50.0))
                if np.isnan(std_14d) or std_14d < umbral:
                    y_final, decision = y_naive, "naive"
                else:
                    y_final, decision = y_rf, "rf"

            precio_real = precio_real_map.get(str(fecha_pred))

            predicciones.append(
                {
                    "Fecha": str(fecha_pred),
                    "Precio_Pred": round(y_final, 2),
                    "Precio_RF": round(y_rf, 2) if not np.isnan(y_rf) else None,
                    "Precio_Naive": round(y_naive, 2),
                    "Decision": decision,
                    "std_14d": round(std_14d, 2) if not np.isnan(std_14d) else None,
                    "Umbral": float(self.umbrales.get(anio_pred, float("nan"))),
                    "dias_bajando": dias_bajando,
                    "Precio_Real": round(float(precio_real), 2) if precio_real is not None and not pd.isna(precio_real) else None,
                    "es_futuro": fecha_pred > ultima_hist,
                }
            )

            fecha_key = str(fecha_pred)
            if fecha_pred <= ultima_hist and fecha_key in actual_rows_by_date.index:
                nueva = actual_rows_by_date.loc[[fecha_key]].copy()
            else:
                nueva = fila.copy()
                nueva["fecha"] = ts_pred
                nueva["Precio_mean"] = y_final
                nueva["precio_lag_1d_log"] = np.log1p(max(y_naive, 0.01))
                for column in TIPOS_VALIDOS + ["Demanda_dia", "embalse_pct_nacional"]:
                    if column in df_pred_ctx.columns:
                        nueva[column] = df_pred_ctx[column].iloc[-1]
                nueva["es_fin_semana"] = int(pd.Timestamp(fecha_pred).weekday() >= 5)
            df_pred_ctx = pd.concat([df_pred_ctx, nueva], ignore_index=True)

        return pd.DataFrame(predicciones)

    def predecir(
        self,
        modo: str = "B",
        dias: int = 7,
        fecha_ini_str: str | None = None,
        fecha_fin_str: str | None = None,
    ) -> dict:
        if not self.listo:
            return {"ok": False, "error": self.error_carga or "Modelo no disponible"}

        with self._lock:
            status_summary = self._build_status_summary()
            latest = status_summary["latest"]
            historical_start = datetime.strptime(self.runtime_config.historical_start_date, "%Y-%m-%d").date()
            dataset_anchor_start = datetime.strptime(self.runtime_config.dataset_anchor_start_date, "%Y-%m-%d").date()
            historical_end = min(dataset_anchor_start - timedelta(days=1), latest) if latest else None

            if latest is None:
                return {
                    "ok": False,
                    "error": "No hay datos cargados en DynamoDB para generar la prediccion",
                    "sync_ready_for_prediction": False,
                    "last_synced_date": status_summary["last_synced_date"],
                    "backfill_status": status_summary["backfill_status"],
                    "last_sync_at": status_summary["last_sync_at"],
                    "last_sync_range": status_summary["last_sync_range"],
                }

            try:
                if modo == "A":
                    dias = max(1, min(dias, 90))
                    if historical_end is None or historical_end < historical_start:
                        return {
                            "ok": False,
                            "error": (
                                f"Historico disponible entre {self.runtime_config.historical_start_date} "
                                f"y {(historical_end.isoformat() if historical_end else 'sin datos')}"
                            ),
                        }
                    fecha_fin_pred = historical_end
                    fecha_ini_pred = historical_end - timedelta(days=dias - 1)
                    if fecha_ini_pred < historical_start:
                        return {
                            "ok": False,
                            "error": (
                                f"Historico disponible entre {self.runtime_config.historical_start_date} "
                                f"y {historical_end.isoformat()}"
                            ),
                        }
                elif modo == "B":
                    if not status_summary["sync_ready_for_prediction"]:
                        return {
                            "ok": False,
                            "error": "sync_incomplete",
                            "sync_ready_for_prediction": False,
                            "last_synced_date": status_summary["last_synced_date"],
                            "backfill_status": status_summary["backfill_status"],
                            "last_sync_at": status_summary["last_sync_at"],
                            "last_sync_range": status_summary["last_sync_range"],
                        }
                    dias = max(1, min(dias, 60))
                    fecha_ini_pred = latest + timedelta(days=1)
                    fecha_fin_pred = latest + timedelta(days=dias)
                elif modo == "C":
                    if not fecha_ini_str or not fecha_fin_str:
                        return {"ok": False, "error": "Modo C requiere fecha_inicio y fecha_fin"}
                    fecha_ini_pred = datetime.strptime(fecha_ini_str, "%Y-%m-%d").date()
                    fecha_fin_pred = datetime.strptime(fecha_fin_str, "%Y-%m-%d").date()
                    dias = (fecha_fin_pred - fecha_ini_pred).days + 1
                    if fecha_fin_pred > latest and not status_summary["sync_ready_for_prediction"]:
                        return {
                            "ok": False,
                            "error": "sync_incomplete",
                            "sync_ready_for_prediction": False,
                            "last_synced_date": status_summary["last_synced_date"],
                            "backfill_status": status_summary["backfill_status"],
                            "last_sync_at": status_summary["last_sync_at"],
                            "last_sync_range": status_summary["last_sync_range"],
                        }
                else:
                    return {"ok": False, "error": f"Modo desconocido: {modo}"}

                df_ext = self._build_prediction_context(fecha_ini_pred, fecha_fin_pred)
                df_ext = recalcular_features(df_ext, self.feats)
                df_pred = self._predecir_loop(df_ext, fecha_ini_pred, dias, latest)

                metricas = self._build_execution_metrics(df_pred) or self._build_reference_metrics()

                return _nan_safe(
                    {
                        "ok": True,
                        "modo": modo,
                        "fecha_inicio": str(fecha_ini_pred),
                        "fecha_fin": str(fecha_fin_pred),
                        "dias": dias,
                        "es_futuro": fecha_fin_pred > latest,
                        "ultima_hist": str(latest),
                        "ultima_hist_dynamodb": str(latest),
                        "n_rf": int((df_pred["Decision"] == "rf").sum()),
                        "n_naive": int((df_pred["Decision"] != "rf").sum()),
                        "predicciones": df_pred.to_dict(orient="records"),
                        "metricas": metricas,
                        "ref_mape_hib": self.config.get("mape_hib", 9.32),
                        "ref_mape_naive": self.config.get("mape_naive", 13.74),
                        "data_source": "dynamodb",
                        "synced_range": None,
                        "missing_days_filled": 0,
                        "sync_ready_for_prediction": True,
                        "backfill_status": status_summary["backfill_status"],
                        "last_synced_date": status_summary["last_synced_date"],
                    }
                )
            except Exception as exc:
                log.exception("Error en prediccion")
                return {"ok": False, "error": str(exc)}

    def status(self) -> dict:
        if not self.listo:
            return {"listo": False, "error": self.error_carga}

        status_summary = self._build_status_summary()
        latest = status_summary["latest"]
        return {
            "listo": True,
            "version": self.config.get("version", "v8"),
            "features": len(self.feats),
            "ultima_hist": str(latest) if latest else None,
            "ultima_hist_dynamodb": str(latest) if latest else None,
            "desfase_dias": (date.today() - latest).days if latest else None,
            "bucket": self.runtime_config.bucket_name,
            "model_key": self.runtime_config.model_object_key,
            "config_key": self.runtime_config.config_object_key,
            "model_in_s3": self.s3_repository.object_exists(self.runtime_config.model_object_key),
            "config_in_s3": self.s3_repository.object_exists(self.runtime_config.config_object_key),
            "raw_prefix": self.runtime_config.raw_prefix,
            "mape_ref": self.config.get("mape_hib", 9.32),
            "last_sync_at": status_summary["last_sync_at"],
            "last_sync_range": status_summary["last_sync_range"],
            "last_sync_mode": status_summary["last_sync_mode"],
            "last_synced_date": status_summary["last_synced_date"],
            "backfill_status": status_summary["backfill_status"],
            "daily_sync_status": status_summary["daily_sync_status"],
            "sync_ready_for_prediction": status_summary["sync_ready_for_prediction"],
            "history_initialized": latest is not None,
            "last_sync_error": status_summary["last_error"],
        }

    def historico(self, dias: int = 60) -> list[dict]:
        if not self.listo:
            return []
        tail = self.feature_repository.get_latest_rows(dias)
        if tail.empty:
            return []
        keep_cols = [
            "fecha",
            "Precio_mean",
            "embalse_pct_nacional",
            "Hidraulica",
            "Termica",
            "Demanda_dia",
        ]
        for column in keep_cols:
            if column not in tail.columns:
                tail[column] = 0
        tail["fecha"] = pd.to_datetime(tail["fecha"]).dt.strftime("%Y-%m-%d")
        return tail[keep_cols].fillna(0).round(2).to_dict(orient="records")
