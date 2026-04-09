from __future__ import annotations

from dataclasses import dataclass

from constructs import Construct


@dataclass(frozen=True)
class ProjectConfig:
    project_name: str
    aws_region: str
    instance_type: str
    allowed_ssh_cidr: str
    app_port: int
    model_bucket_prefix: str
    model_object_key: str
    config_object_key: str
    raw_prefix: str
    missing_data_threshold_days: int
    lookback_days: int
    sync_status_table_name: str
    initial_backfill_start_date: str
    historical_start_date: str
    dataset_anchor_start_date: str
    daily_sync_lookback_days: int
    sync_schedule_time: str
    backfill_chunk_days: int
    key_name: str
    vpc_cidr: str


def _context_str(scope: Construct, key: str, default: str) -> str:
    value = scope.node.try_get_context(key)
    if value is None:
        return default
    return str(value)


def _context_int(scope: Construct, key: str, default: int) -> int:
    value = scope.node.try_get_context(key)
    if value is None:
        return default
    return int(value)


def load_project_config(scope: Construct) -> ProjectConfig:
    return ProjectConfig(
        project_name=_context_str(scope, "project_name", "free-tier-ml-demo"),
        aws_region=_context_str(scope, "aws_region", "us-east-1"),
        instance_type=_context_str(scope, "instance_type", "t3.micro"),
        allowed_ssh_cidr=_context_str(scope, "allowed_ssh_cidr", "0.0.0.0/0"),
        app_port=_context_int(scope, "app_port", 8000),
        model_bucket_prefix=_context_str(scope, "model_bucket_prefix", "free-tier-ml-artifacts"),
        model_object_key=_context_str(scope, "model_object_key", "models/rf_hibrido_v5.pkl"),
        config_object_key=_context_str(scope, "config_object_key", "config/config_v5.pkl"),
        raw_prefix=_context_str(scope, "raw_prefix", "raw/simem"),
        missing_data_threshold_days=_context_int(scope, "missing_data_threshold_days", 3),
        lookback_days=_context_int(scope, "lookback_days", 60),
        sync_status_table_name=_context_str(scope, "sync_status_table_name", "sync-status"),
        initial_backfill_start_date=_context_str(scope, "initial_backfill_start_date", "2023-01-01"),
        historical_start_date=_context_str(scope, "historical_start_date", "2023-01-01"),
        dataset_anchor_start_date=_context_str(scope, "dataset_anchor_start_date", "2023-07-31"),
        daily_sync_lookback_days=_context_int(scope, "daily_sync_lookback_days", 7),
        sync_schedule_time=_context_str(scope, "sync_schedule_time", "08:00"),
        backfill_chunk_days=_context_int(scope, "backfill_chunk_days", 3),
        key_name=_context_str(scope, "key_name", ""),
        vpc_cidr=_context_str(scope, "vpc_cidr", "10.42.0.0/24"),
    )
