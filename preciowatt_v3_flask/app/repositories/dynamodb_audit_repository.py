from __future__ import annotations

import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .dynamodb_utils import to_dynamodb_compatible


logger = logging.getLogger(__name__)


class PredictionAuditRepository:
    def __init__(self, *, region_name: str, table_name: str) -> None:
        self._table = None
        if table_name:
            dynamodb = boto3.resource("dynamodb", region_name=region_name)
            self._table = dynamodb.Table(table_name)

    def put_record(self, record: dict[str, object]) -> None:
        if self._table is None:
            return
        try:
            normalized = {key: to_dynamodb_compatible(value) for key, value in record.items()}
            self._table.put_item(Item=normalized)
        except (ClientError, BotoCoreError) as exc:
            logger.warning("Failed to write prediction audit record: %s", exc)
