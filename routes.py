from flask import Blueprint, jsonify, render_template

from db import raw_collection, processed_collection
from fetch_data import fetch_opensky_data
from pipeline import run_pipeline, store_raw_data
from preprocess import process_opensky_response

routes = Blueprint("routes", __name__)


@routes.route("/", methods=["GET"])
def home():
    return render_template("index.html")


@routes.route("/api/status", methods=["GET"])
def status():
    return jsonify({
        "message": "OpenSky Pipeline API is running"
    })


@routes.route("/fetch-data", methods=["GET"])
def fetch_data_route():
    raw_data = fetch_opensky_data()
    raw_id = store_raw_data(raw_data)

    return jsonify({
        "message": "Raw OpenSky data fetched and stored",
        "raw_document_id": raw_id,
        "raw_records_count": len(raw_data.get("states", [])),
    })


@routes.route("/process-data", methods=["GET"])
def process_data_route():
    latest_raw = raw_collection.find_one(sort=[("_id", -1)])

    if not latest_raw:
        return jsonify({"error": "No raw data found. Fetch data first."}), 404

    raw_data = {
        "time": latest_raw.get("time"),
        "states": latest_raw.get("states", []),
    }

    processed_records = process_opensky_response(raw_data)

    if processed_records:
        processed_collection.insert_many(processed_records)

    return jsonify({
        "message": "Latest raw data processed and stored",
        "processed_records_count": len(processed_records),
    })


@routes.route("/run-pipeline", methods=["GET"])
def run_pipeline_route():
    result = run_pipeline()
    return jsonify(result)


@routes.route("/raw-data", methods=["GET"])
def get_raw_data():
    documents = list(raw_collection.find({}, {"states": 0}))
    for doc in documents:
        doc["_id"] = str(doc["_id"])
    return jsonify(documents)


@routes.route("/processed-data", methods=["GET"])
def get_processed_data():
    documents = list(processed_collection.find().sort("_id", -1).limit(100))

    for doc in documents:
        doc["_id"] = str(doc["_id"])

    return jsonify(documents)