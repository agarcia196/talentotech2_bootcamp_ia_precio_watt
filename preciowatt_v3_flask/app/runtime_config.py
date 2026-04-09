from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RuntimeConfig:
    aws_region: str
    bucket_name: str
    model_object_key: str
    config_object_key: str
    raw_prefix: str
    daily_feature_table_name: str
    audit_table_name: str
    sync_status_table_name: str
    missing_data_threshold_days: int
    lookback_days: int
    initial_backfill_start_date: str
    historical_start_date: str
    dataset_anchor_start_date: str
    daily_sync_lookback_days: int
    sync_schedule_time: str
    backfill_chunk_days: int
    bootstrap_from_local_assets: bool
    base_dir: Path

    @property
    def model_local_path(self) -> Path:
        return self.base_dir / "model_assets" / "rf_hibrido_v5.pkl"

    @property
    def config_local_path(self) -> Path:
        return self.base_dir / "model_assets" / "config_v5.pkl"

    @property
    def bootstrap_dataset_path(self) -> Path:
        return self.base_dir / "model_assets" / "dataset_diario_2023_2026_marzo.csv"

    @classmethod
    def from_env(cls, *, base_dir: Path) -> "RuntimeConfig":
        return cls(
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
            bucket_name=os.getenv("APP_BUCKET_NAME", ""),
            model_object_key=os.getenv("MODEL_OBJECT_KEY", "models/rf_hibrido_v5.pkl"),
            config_object_key=os.getenv("CONFIG_OBJECT_KEY", "config/config_v5.pkl"),
            raw_prefix=os.getenv("RAW_PREFIX", "raw/simem"),
            daily_feature_table_name=os.getenv("DAILY_FEATURE_TABLE_NAME", ""),
            audit_table_name=os.getenv("PREDICTION_AUDIT_TABLE_NAME", ""),
            sync_status_table_name=os.getenv("SYNC_STATUS_TABLE_NAME", ""),
            missing_data_threshold_days=int(os.getenv("MISSING_DATA_THRESHOLD_DAYS", "3")),
            lookback_days=int(os.getenv("LOOKBACK_DAYS", "60")),
            initial_backfill_start_date=os.getenv("INITIAL_BACKFILL_START_DATE", "2023-01-01"),
            historical_start_date=os.getenv("HISTORICAL_START_DATE", "2023-01-01"),
            dataset_anchor_start_date=os.getenv("DATASET_ANCHOR_START_DATE", "2023-07-31"),
            daily_sync_lookback_days=int(os.getenv("DAILY_SYNC_LOOKBACK_DAYS", "7")),
            sync_schedule_time=os.getenv("SYNC_SCHEDULE_TIME", "08:00"),
            backfill_chunk_days=int(os.getenv("BACKFILL_CHUNK_DAYS", "3")),
            bootstrap_from_local_assets=os.getenv("BOOTSTRAP_FROM_LOCAL_ASSETS", "true").lower() == "true",
            base_dir=base_dir,
        )
