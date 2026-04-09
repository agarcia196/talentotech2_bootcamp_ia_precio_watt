from __future__ import annotations

import ctypes
import gc
import logging
from datetime import date, datetime, timedelta

import pandas as pd

from ..repositories.dynamodb_feature_repository import BASE_COLUMNS, DailyFeatureRepository
from ..repositories.s3_asset_repository import S3AssetRepository
from ..repositories.sync_status_repository import SyncStatusRepository
from ..runtime_config import RuntimeConfig
from .simem_client import SimemClient


logger = logging.getLogger("sync_service")
IGNORABLE_SIMEM_ERROR = "SIMEM returned insufficient data for the requested range"


def _release_memory() -> None:
    gc.collect()
    try:
        libc = ctypes.CDLL("libc.so.6")
        libc.malloc_trim(0)
    except Exception:
        pass


def _is_ignorable_simem_error(exc: Exception) -> bool:
    return IGNORABLE_SIMEM_ERROR in str(exc)


class SyncService:
    BACKFILL_SYNC_NAME = "backfill"
    DAILY_SYNC_NAME = "daily"

    def __init__(self, runtime_config: RuntimeConfig) -> None:
        self.runtime_config = runtime_config
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
        self.simem_client = SimemClient()

    def get_sync_record(self, sync_name: str) -> dict[str, object] | None:
        return self.sync_status_repository.get_record(sync_name)

    def get_backfill_record(self) -> dict[str, object] | None:
        return self.get_sync_record(self.BACKFILL_SYNC_NAME)

    def get_daily_record(self) -> dict[str, object] | None:
        return self.get_sync_record(self.DAILY_SYNC_NAME)

    def maybe_run_backfill(self) -> dict[str, object]:
        current = self.get_backfill_record() or {}
        if current.get("status") == "completed":
            logger.info("Backfill already completed; skipping.")
            return {"started": False, "reason": "already_completed"}
        if current.get("status") == "in_progress":
            logger.info("Backfill already in progress; skipping duplicate launch.")
            return {"started": False, "reason": "already_in_progress"}
        return self.run_backfill()

    def run_backfill(self) -> dict[str, object]:
        configured_start = datetime.strptime(
            self.runtime_config.initial_backfill_start_date, "%Y-%m-%d"
        ).date()
        current = self.get_backfill_record() or {}
        last_synced = current.get("last_synced_date")
        if last_synced:
            start_date = datetime.strptime(str(last_synced), "%Y-%m-%d").date() + timedelta(days=1)
        else:
            start_date = configured_start
        end_date = date.today()
        if start_date > end_date:
            self.sync_status_repository.put_record(
                self.BACKFILL_SYNC_NAME,
                {
                    "status": "completed",
                    "sync_mode": "backfill",
                    "last_success_at": datetime.utcnow().isoformat(),
                    "last_synced_date": end_date.isoformat(),
                },
            )
            logger.info("Backfill already fully synchronized through %s", end_date)
            return {"started": False, "reason": "already_up_to_date"}

        self._run_sync(
            sync_name=self.BACKFILL_SYNC_NAME,
            sync_mode="backfill",
            start_date=start_date,
            end_date=end_date,
        )
        return {"started": True, "sync_name": self.BACKFILL_SYNC_NAME}

    def run_daily(self) -> dict[str, object]:
        backfill = self.get_backfill_record() or {}
        if backfill.get("status") == "in_progress":
            logger.info("Daily sync skipped because backfill is in progress.")
            return {"started": False, "reason": "backfill_in_progress"}

        end_date = date.today()
        configured_start = datetime.strptime(
            self.runtime_config.initial_backfill_start_date, "%Y-%m-%d"
        ).date()
        start_date = max(
            configured_start,
            end_date - timedelta(days=self.runtime_config.daily_sync_lookback_days),
        )
        self._run_sync(
            sync_name=self.DAILY_SYNC_NAME,
            sync_mode="daily",
            start_date=start_date,
            end_date=end_date,
        )
        return {"started": True, "sync_name": self.DAILY_SYNC_NAME}

    def _run_sync(
        self,
        *,
        sync_name: str,
        sync_mode: str,
        start_date: date,
        end_date: date,
    ) -> None:
        processed_start = start_date.isoformat()
        processed_end = end_date.isoformat()
        self.sync_status_repository.mark_started(
            sync_name,
            sync_mode=sync_mode,
            processed_start=processed_start,
            processed_end=processed_end,
        )

        try:
            logger.info(
                "Starting sync sync_name=%s sync_mode=%s start=%s end=%s",
                sync_name,
                sync_mode,
                start_date,
                end_date,
            )
            if sync_mode == "backfill":
                latest = self._run_backfill_in_chunks(
                    sync_name=sync_name,
                    start_date=start_date,
                    end_date=end_date,
                )
            else:
                latest = self._run_single_window(
                    sync_mode=sync_mode,
                    start_date=start_date,
                    end_date=end_date,
                )

            self.sync_status_repository.mark_success(
                sync_name,
                sync_mode=sync_mode,
                processed_start=processed_start,
                processed_end=processed_end,
                last_synced_date=latest.isoformat() if latest else None,
            )
            logger.info(
                "Sync completed sync_name=%s sync_mode=%s last_synced_date=%s",
                sync_name,
                sync_mode,
                latest,
            )
        except Exception as exc:
            self.sync_status_repository.safe_mark_failed(
                sync_name,
                sync_mode=sync_mode,
                processed_start=processed_start,
                processed_end=processed_end,
                error_message=str(exc).strip() or repr(exc),
            )
            logger.exception("Sync failed sync_name=%s sync_mode=%s", sync_name, sync_mode)
            raise

    def _run_backfill_in_chunks(
        self,
        *,
        sync_name: str,
        start_date: date,
        end_date: date,
    ) -> date | None:
        chunk_days = max(1, self.runtime_config.backfill_chunk_days)
        current_start = start_date
        latest: date | None = self.feature_repository.get_latest_date()

        while current_start <= end_date:
            current_end = min(current_start + timedelta(days=chunk_days - 1), end_date)
            logger.info(
                "Processing backfill chunk start=%s end=%s chunk_days=%s",
                current_start,
                current_end,
                chunk_days,
            )
            try:
                latest = self._run_single_window(
                    sync_mode="backfill",
                    start_date=current_start,
                    end_date=current_end,
                )
            except Exception as exc:
                if not _is_ignorable_simem_error(exc):
                    raise
                logger.warning(
                    "Skipping backfill chunk with insufficient SIMEM data start=%s end=%s error=%s",
                    current_start,
                    current_end,
                    exc,
                )
                latest = self.feature_repository.get_latest_date()
            self.sync_status_repository.put_record(
                sync_name,
                {
                    "status": "in_progress",
                    "sync_mode": "backfill",
                    "processed_start": start_date.isoformat(),
                    "processed_end": end_date.isoformat(),
                    "last_synced_date": current_end.isoformat(),
                },
            )
            current_start = current_end + timedelta(days=1)
            _release_memory()

        return latest

    def _run_single_window(
        self,
        *,
        sync_mode: str,
        start_date: date,
        end_date: date,
    ) -> date | None:
        try:
            daily_frame, raw_payload = self.simem_client.fetch_context_range(
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:
            if _is_ignorable_simem_error(exc):
                logger.warning(
                    "Ignoring sync window with insufficient SIMEM data sync_mode=%s start=%s end=%s",
                    sync_mode,
                    start_date,
                    end_date,
                )
                return self.feature_repository.get_latest_date()
            raise
        if daily_frame.empty:
            logger.warning(
                "Ignoring sync window with no daily rows sync_mode=%s start=%s end=%s",
                sync_mode,
                start_date,
                end_date,
            )
            return self.feature_repository.get_latest_date()

        daily_frame["fecha"] = pd.to_datetime(daily_frame["fecha"])
        daily_frame = self._merge_embalse(daily_frame, start_date, end_date)

        run_ts = self.s3_repository.build_run_timestamp()
        for dataset_name, dataset_frame in raw_payload.items():
            if dataset_frame is None or dataset_frame.empty:
                continue
            self.s3_repository.save_raw_dataframe(
                dataset_name=dataset_name,
                start_date=start_date,
                end_date=end_date,
                run_ts=run_ts,
                dataframe=dataset_frame,
                raw_prefix=self.runtime_config.raw_prefix,
            )
            del dataset_frame

        updated_at = datetime.utcnow().isoformat()
        self.feature_repository.upsert_rows(
            daily_frame[BASE_COLUMNS].copy(),
            source_range=f"{start_date.isoformat()}:{end_date.isoformat()}",
            sync_mode=sync_mode,
            updated_at=updated_at,
        )

        del raw_payload
        del daily_frame
        _release_memory()
        return self.feature_repository.get_latest_date()

    def _merge_embalse(self, frame: pd.DataFrame, start_date: date, end_date: date) -> pd.DataFrame:
        if frame.empty:
            return frame

        support_start = start_date - timedelta(days=30)
        support_end = end_date
        existing = self.feature_repository.get_rows_in_range(support_start, support_end)
        if existing.empty or "embalse_pct_nacional" not in existing.columns:
            frame["embalse_pct_nacional"] = 0.0
            return frame

        reference = (
            existing[["fecha", "embalse_pct_nacional"]]
            .dropna(subset=["fecha"])
            .sort_values("fecha")
            .drop_duplicates("fecha", keep="last")
            .reset_index(drop=True)
        )
        mapping = {
            pd.Timestamp(row["fecha"]).strftime("%Y-%m-%d"): row["embalse_pct_nacional"]
            for _, row in reference.iterrows()
        }
        frame = frame.copy()
        frame["embalse_pct_nacional"] = frame["fecha"].dt.strftime("%Y-%m-%d").map(mapping)
        frame["embalse_pct_nacional"] = frame["embalse_pct_nacional"].ffill().bfill().fillna(0.0)
        return frame
