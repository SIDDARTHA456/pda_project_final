import os

class Config:
    # OpenSky free API endpoint
    OPENSKY_BASE_URL = "https://opensky-network.org/api/states/all"

    # MongoDB Atlas connection string
    # Replace this with your actual cluster connection string
    MONGO_URI = os.getenv(
        "MONGO_URI",
        "mongodb+srv://siddumanda0507_db_user:78RMZqeQw73fPl3B@cluster0.8r2x7ks.mongodb.net/"
    )

    DATABASE_NAME = "opensky_pipeline_db"
    RAW_COLLECTION = "raw_states"
    PROCESSED_COLLECTION = "processed_states"

    # Bounding box for Dublin region (edit if needed)
    LAMIN = 53.20
    LOMIN = -6.50
    LAMAX = 53.50
    LOMAX = -6.00

    # Thresholds
    STALE_SECONDS = 15
    LOW_SPEED_KMH = 200
    HIGH_SPEED_KMH = 600
    LOW_ALTITUDE_M = 3000
    HIGH_ALTITUDE_M = 9000