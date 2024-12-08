import logging
from datetime import datetime
from hashlib import sha256
from typing import Any, Dict, Optional, Tuple

import pendulum

logger = logging.getLogger(__name__)


def validate_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Validate a single record against business rules"""
    try:
        if not isinstance(record.get("price"), (int, float)):
            return False, "Invalid price type"
        if record.get("price", 0) < 0:
            return False, "Negative price"
        if not record.get("product_id"):
            return False, "Missing product_id"
        return True, None
    except Exception as e:
        return False, str(e)


def generate_record_hash(record: Dict[str, Any]) -> str:
    """Generate a unique hash for a record based on business keys"""
    key_fields = ["product_id", "timestamp", "user_id"]
    key_string = "|".join(str(record.get(field, "")) for field in key_fields)
    return sha256(key_string.encode()).hexdigest()


def enrich_record(record: Dict[str, Any], record_hash: str) -> Dict[str, Any]:
    """Add metadata to a record"""
    record["processed_date"] = datetime.now(tz=pendulum.timezone("UTC")).isoformat()
    record["processing_pipeline"] = "minio_etl"
    record["valid"] = "TRUE"
    record["record_hash"] = record_hash
    return record
