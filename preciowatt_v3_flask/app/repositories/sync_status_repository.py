from __future__ import annotations

import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .dynamodb_utils import from_dynamodb_compatible, to_dynamodb_compatible


logger = logging.getLogger(__name__)


class SyncStatusRepository:
    def __init__(self, *, region_name: str, table_name: str) -> None:
        self._table = None
        if table_name:
            dynamodb = boto3.resource("dynamodb", region_name=region_name)
            self._table = dynamodb.Table(table_name)

    def _require_table(self):
        if self._table is None:
            raise RuntimeError("SYNC_STATUS_TABLE_NAME is not configured")
        return self._table

    def get_record(self, sync_name: str) -> dict[str, object] | None:
        table = self._require_table()
        response = table.get_item(Key={"sync_name": sync_name})
        item = response.get("Item")
        if not item:
            return None
        return {k: from_dynamodb_compatible(v) for k, v in item.items()}

    def put_record(self, sync_name: str, values: dict[str, object]) -> None:
        table = self._require_table()
        current = self.get_record(sync_name) or {"sync_name": sync_name}
        current.update(values)
        normalized = {k: to_dynamodb_compatible(v) for k, v in current.items()}
        table.put_item(Item=normalized)

    def mark_started(self, sync_name: str, *, sync_mode: str, processed_start: str, processed_end: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.put_record(
            sync_name,
            {
                "sync_name": sync_name,
                "status": "in_progress",
                "sync_mode": sync_mode,
                "started_at": now,
                "finished_at": None,
                "processed_start": processed_start,
                "processed_end": processed_end,
                "last_error": None,
            },
        )

    def mark_success(
        self,
        sync_name: str,
        *,
        sync_mode: str,
        processed_start: str,
        processed_end: str,
        last_synced_date: str | None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.put_record(
            sync_name,
            {
                "sync_name": sync_name,
                "status": "completed",
                "sync_mode": sync_mode,
                "finished_at": now,
                "last_success_at": now,
                "processed_start": processed_start,
                "processed_end": processed_end,
                "last_synced_date": last_synced_date,
                "last_error": None,
            },
        )

    def mark_failed(
        self,
        sync_name: str,
        *,
        sync_mode: str,
        processed_start: str,
        processed_end: str,
        error_message: str,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.put_record(
            sync_name,
            {
                "sync_name": sync_name,
                "status": "failed",
                "sync_mode": sync_mode,
                "finished_at": now,
                "processed_start": processed_start,
                "processed_end": processed_end,
                "last_error": error_message[:2000],
            },
        )

    def safe_mark_failed(
        self,
        sync_name: str,
        *,
        sync_mode: str,
        processed_start: str,
        processed_end: str,
        error_message: str,
    ) -> None:
        try:
            self.mark_failed(
                sync_name,
                sync_mode=sync_mode,
                processed_start=processed_start,
                processed_end=processed_end,
                error_message=error_message,
            )
        except (ClientError, BotoCoreError) as exc:
            logger.warning("Failed to persist sync failure for %s: %s", sync_name, exc)
