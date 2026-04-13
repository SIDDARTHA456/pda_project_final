import requests
from config import Config

def fetch_opensky_data() -> dict:
    """
    Fetch live aircraft state vectors from OpenSky API using bounding box.
    Returns raw JSON response.
    Raises requests.HTTPError on failure.
    """
    params = {
        "lamin": Config.LAMIN,
        "lomin": Config.LOMIN,
        "lamax": Config.LAMAX,
        "lomax": Config.LOMAX,
    }

    response = requests.get(Config.OPENSKY_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()