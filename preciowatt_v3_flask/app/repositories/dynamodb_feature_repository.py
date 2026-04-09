from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key

from .dynamodb_utils import from_dynamodb_compatible, to_dynamodb_compatible


logger = logging.getLogger(__name__)


BASE_COLUMNS = [
    "fecha",
    "Precio_mean",
    "Demanda_dia",
    "Hidraulica",
    "Termica",
    "Solar",
    "Eolica",
    "Cogenerador",
    "embalse_pct_nacional",
]


class DailyFeatureRepository:
    RECORD_TYPE = "daily_feature"

    def __init__(self, *, region_name: str, table_name: str) -> None:
        self.table_name = table_name
        self._table = None
        if table_name:
            dynamodb = boto3.resource("dynamodb", region_name=region_name)
            self._table = dynamodb.Table(table_name)

    def _require_table(self):
        if self._table is None:
            raise RuntimeError("DAILY_FEATURE_TABLE_NAME is not configured")
        return self._table

    def is_empty(self) -> bool:
        table = self._require_table()
        response = table.query(
            KeyConditionExpression=Key("record_type").eq(self.RECORD_TYPE),
            Limit=1,
        )
        return not response.get("Items")

    def get_latest_date(self) -> date | None:
        table = self._require_table()
        response = table.query(
            KeyConditionExpression=Key("record_type").eq(self.RECORD_TYPE),
            ScanIndexForward=False,
            Limit=1,
        )
        items = response.get("Items", [])
        if not items:
            return None
        return datetime.strptime(items[0]["date"], "%Y-%m-%d").date()

    def get_earliest_date(self) -> date | None:
        table = self._require_table()
        response = table.query(
            KeyConditionExpression=Key("record_type").eq(self.RECORD_TYPE),
            ScanIndexForward=True,
            Limit=1,
        )
        items = response.get("Items", [])
        if not items:
            return None
        return datetime.strptime(items[0]["date"], "%Y-%m-%d").date()

    def get_latest_sync_metadata(self) -> dict[str, object] | None:
        table = self._require_table()
        response = table.query(
            KeyConditionExpression=Key("record_type").eq(self.RECORD_TYPE),
            ScanIndexForward=False,
            Limit=1,
        )
        items = response.get("Items", [])
        if not items:
            return None
        item = {k: from_dynamodb_compatible(v) for k, v in items[0].items()}
        return {
            "date": item.get("date"),
            "updated_at": item.get("updated_at"),
            "source_range": item.get("source_range"),
            "sync_mode": item.get("sync_mode"),
        }

    def get_rows_in_range(self, start_date: date, end_date: date) -> pd.DataFrame:
        table = self._require_table()
        items: list[dict[str, object]] = []
        last_evaluated_key = None

        while True:
            params = {
                "KeyConditionExpression": Key("record_type").eq(self.RECORD_TYPE)
                & Key("date").between(start_date.isoformat(), end_date.isoformat())
            }
            if last_evaluated_key is not None:
                params["ExclusiveStartKey"] = last_evaluated_key
            response = table.query(**params)
            items.extend(response.get("Items", []))
            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        normalized = [{k: from_dynamodb_compatible(v) for k, v in item.items()} for item in items]
        if not normalized:
            return pd.DataFrame(columns=BASE_COLUMNS + ["source_range", "updated_at", "sync_mode"])

        frame = pd.DataFrame(normalized)
        frame["fecha"] = pd.to_datetime(frame["date"])
        if "embalse_pct_nacional" not in frame.columns:
            frame["embalse_pct_nacional"] = pd.NA
        ordered_cols = [col for col in BASE_COLUMNS if col in frame.columns]
        extra = [col for col in frame.columns if col not in set(ordered_cols + ["date"])]
        return frame[ordered_cols + extra].sort_values("fecha").reset_index(drop=True)

    def get_latest_rows(self, limit: int) -> pd.DataFrame:
        table = self._require_table()
        response = table.query(
            KeyConditionExpression=Key("record_type").eq(self.RECORD_TYPE),
            ScanIndexForward=False,
            Limit=limit,
        )
        items = response.get("Items", [])
        normalized = [{k: from_dynamodb_compatible(v) for k, v in item.items()} for item in items]
        if not normalized:
            return pd.DataFrame(columns=BASE_COLUMNS)
        frame = pd.DataFrame(normalized)
        frame["fecha"] = pd.to_datetime(frame["date"])
        return frame.sort_values("fecha").reset_index(drop=True)

    def find_missing_dates(self, start_date: date, end_date: date) -> list[date]:
        expected_dates = {
            (start_date + timedelta(days=offset))
            for offset in range((end_date - start_date).days + 1)
        }
        existing = self.get_rows_in_range(start_date, end_date)
        existing_dates = set(existing["fecha"].dt.date.tolist()) if not existing.empty else set()
        return sorted(expected_dates - existing_dates)

    def upsert_rows(
        self,
        dataframe: pd.DataFrame,
        *,
        source_range: str,
        sync_mode: str,
        updated_at: str,
    ) -> None:
        table = self._require_table()
        if dataframe.empty:
            return

        with table.batch_writer(overwrite_by_pkeys=["record_type", "date"]) as batch:
            for row in dataframe.to_dict(orient="records"):
                fecha_value = row.get("fecha")
                if pd.isna(fecha_value):
                    continue
                date_str = pd.Timestamp(fecha_value).strftime("%Y-%m-%d")
                item = {
                    "record_type": self.RECORD_TYPE,
                    "date": date_str,
                    "source_range": source_range,
                    "updated_at": updated_at,
                    "sync_mode": sync_mode,
                }
                for column in BASE_COLUMNS:
                    if column == "fecha":
                        continue
                    item[column] = to_dynamodb_compatible(row.get(column))
                batch.put_item(Item=item)

    def seed_from_bootstrap(self, dataframe: pd.DataFrame, *, updated_at: str) -> None:
        self.upsert_rows(
            dataframe,
            source_range="bootstrap_csv",
            sync_mode="bootstrap",
            updated_at=updated_at,
        )
