from __future__ import annotations

import io
import logging
import tempfile
from datetime import date, datetime
from pathlib import Path

import boto3
import joblib
import pandas as pd
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


class S3AssetRepository:
    def __init__(self, *, region_name: str, bucket_name: str) -> None:
        self.region_name = region_name
        self.bucket_name = bucket_name
        self._client = boto3.client("s3", region_name=region_name)
        self._cache_dir = Path(tempfile.gettempdir()) / "preciowatt-s3-cache"
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def _head(self, key: str) -> dict[str, object] | None:
        if not self.bucket_name:
            return None
        try:
            return self._client.head_object(Bucket=self.bucket_name, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    def object_exists(self, key: str) -> bool:
        return self._head(key) is not None

    def ensure_object(self, *, key: str, local_path: Path) -> None:
        if not self.bucket_name or not local_path.exists():
            return
        if self.object_exists(key):
            return
        logger.info("Uploading bootstrap asset to s3://%s/%s", self.bucket_name, key)
        self._client.upload_file(str(local_path), self.bucket_name, key)

    def load_joblib_object(self, key: str):
        if not self.bucket_name:
            raise RuntimeError("APP_BUCKET_NAME is not configured")

        metadata = self._head(key)
        if metadata is None:
            raise FileNotFoundError(f"S3 object not found: s3://{self.bucket_name}/{key}")

        etag = str(metadata.get("ETag", "")).replace('"', "")
        suffix = Path(key).suffix or ".pkl"
        local_path = self._cache_dir / f"{Path(key).stem}-{etag}{suffix}"
        if not local_path.exists():
            self._client.download_file(self.bucket_name, key, str(local_path))
        return joblib.load(local_path)

    def save_raw_dataframe(
        self,
        *,
        dataset_name: str,
        start_date: date,
        end_date: date,
        run_ts: str,
        dataframe: pd.DataFrame,
        raw_prefix: str,
    ) -> str:
        if not self.bucket_name:
            raise RuntimeError("APP_BUCKET_NAME is not configured")

        prefix = raw_prefix.strip("/")
        key = (
            f"{prefix}/{dataset_name}/{start_date:%Y}/{start_date:%m}/"
            f"{dataset_name}_{start_date.isoformat()}_{end_date.isoformat()}_{run_ts}.parquet"
        )

        buffer = io.BytesIO()
        dataframe.to_parquet(buffer, index=False)
        buffer.seek(0)
        self._client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        return key

    @staticmethod
    def build_run_timestamp() -> str:
        return datetime.utcnow().strftime("%Y%m%d_%H%M%S")
