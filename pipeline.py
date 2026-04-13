from datetime import datetime, timezone
from typing import Dict, Any

from db import raw_collection, processed_collection
from fetch_data import fetch_opensky_data
from preprocess import process_opensky_response


def store_raw_data(raw_data: Dict[str, Any]) -> str:
    """
    Store raw OpenSky response in MongoDB.
    """
    raw_document = {
        "source": "OpenSky API",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "time": raw_data.get("time"),
        "states": raw_data.get("states", []),
    }
    result = raw_collection.insert_one(raw_document)
    return str(result.inserted_id)


def store_processed_data(processed_records):
    """
    Store processed records in MongoDB.
    """
    if not processed_records:
        return 0
    result = processed_collection.insert_many(processed_records)
    return len(result.inserted_ids)


def run_pipeline() -> Dict[str, Any]:
    """
    Full pipeline:
    1. Fetch live data
    2. Store raw data
    3. Process data
    4. Store processed data
    """
    raw_data = fetch_opensky_data()
    raw_id = store_raw_data(raw_data)

    processed_records = process_opensky_response(raw_data)
    processed_count = store_processed_data(processed_records)

    return {
        "message": "Pipeline executed successfully",
        "raw_document_id": raw_id,
        "raw_records_count": len(raw_data.get("states", [])),
        "processed_records_count": processed_count,
    }