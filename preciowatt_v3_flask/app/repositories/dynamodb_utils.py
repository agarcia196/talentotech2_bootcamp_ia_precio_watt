from __future__ import annotations

from decimal import Decimal

import numpy as np
import pandas as pd


def to_dynamodb_compatible(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (int,)):
        return value
    if isinstance(value, (np.floating, float)):
        if pd.isna(value):
            return None
        return Decimal(str(float(value)))
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    return value


def from_dynamodb_compatible(value):
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    return value
