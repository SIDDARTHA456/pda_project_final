from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config import Config


def unix_to_utc_string(timestamp: Optional[int]) -> Optional[str]:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def velocity_to_kmh(velocity_mps: Optional[float]) -> float:
    if velocity_mps is None:
        return 0.0
    return round(velocity_mps * 3.6, 2)


def speed_category(speed_kmh: float) -> str:
    if speed_kmh < Config.LOW_SPEED_KMH:
        return "low"
    if speed_kmh <= Config.HIGH_SPEED_KMH:
        return "medium"
    return "high"


def altitude_category(altitude_m: float, on_ground: bool) -> str:
    if on_ground or altitude_m <= 0:
        return "ground"
    if altitude_m < Config.LOW_ALTITUDE_M:
        return "low"
    if altitude_m <= Config.HIGH_ALTITUDE_M:
        return "medium"
    return "high"


def stale_data_flag(current_time: Optional[int], last_contact: Optional[int]) -> bool:
    if current_time is None or last_contact is None:
        return True
    return (current_time - last_contact) > Config.STALE_SECONDS


def normalize_callsign(callsign: Optional[str]) -> str:
    if not callsign:
        return "UNKNOWN"
    return callsign.strip() or "UNKNOWN"


def safe_number(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def flight_status(on_ground: bool) -> str:
    return "on_ground" if on_ground else "airborne"


def flight_phase(
    on_ground: bool,
    altitude_m: float,
    speed_kmh: float,
    vertical_rate: float,
) -> str:
    """
    Rule-based flight phase classification.
    """
    if on_ground and speed_kmh < 5:
        return "parked"
    if on_ground and speed_kmh >= 5:
        return "taxi"
    if not on_ground and altitude_m < 1000 and vertical_rate > 2:
        return "takeoff"
    if not on_ground and vertical_rate > 1 and altitude_m < Config.HIGH_ALTITUDE_M:
        return "climb"
    if not on_ground and altitude_m >= Config.HIGH_ALTITUDE_M and abs(vertical_rate) <= 1:
        return "cruise"
    if not on_ground and vertical_rate < -1 and altitude_m > 1000:
        return "descent"
    if not on_ground and altitude_m <= 1000 and vertical_rate < 0:
        return "landing"
    return "unknown"


def anomaly_flag(
    on_ground: bool,
    altitude_m: float,
    speed_kmh: float,
    vertical_rate: float,
) -> bool:
    """
    Simple anomaly rules.
    """
    if not on_ground and speed_kmh == 0:
        return True
    if altitude_m > 0 and on_ground:
        return True
    if abs(vertical_rate) > 50:
        return True
    if speed_kmh > 1200:
        return True
    return False


def clean_and_transform_state(state: List[Any], current_time: Optional[int]) -> Dict[str, Any]:
    """
    OpenSky state vector index mapping:
    0  icao24
    1  callsign
    2  origin_country
    3  time_position
    4  last_contact
    5  longitude
    6  latitude
    7  baro_altitude
    8  on_ground
    9  velocity
    10 true_track
    11 vertical_rate
    12 sensors
    13 geo_altitude
    14 squawk
    15 spi
    16 position_source
    """

    icao24 = state[0]
    callsign = normalize_callsign(state[1])
    origin_country = state[2]
    time_position = state[3]
    last_contact = state[4]
    longitude = state[5]
    latitude = state[6]
    baro_altitude = safe_number(state[7], 0.0)
    on_ground = bool(state[8]) if state[8] is not None else False
    velocity = safe_number(state[9], 0.0)
    heading = safe_number(state[10], 0.0)
    vertical_rate = safe_number(state[11], 0.0)
    geo_altitude = safe_number(state[13], 0.0)
    squawk = state[14] if state[14] is not None else "UNKNOWN"

    altitude = geo_altitude if geo_altitude > 0 else baro_altitude
    speed_kmh_value = velocity_to_kmh(velocity)

    processed = {
        "icao24": icao24,
        "callsign": callsign,
        "origin_country": origin_country,
        "time_position": time_position,
        "last_contact": last_contact,
        "datetime_utc": unix_to_utc_string(last_contact),
        "longitude": longitude,
        "latitude": latitude,
        "baro_altitude": baro_altitude,
        "geo_altitude": geo_altitude,
        "altitude_used": altitude,
        "on_ground": on_ground,
        "velocity_mps": velocity,
        "velocity_kmh": speed_kmh_value,
        "heading": heading,
        "vertical_rate": vertical_rate,
        "squawk": squawk,
        "flight_status": flight_status(on_ground),
        "speed_category": speed_category(speed_kmh_value),
        "altitude_category": altitude_category(altitude, on_ground),
        "stale_data_flag": stale_data_flag(current_time, last_contact),
        "flight_phase": flight_phase(on_ground, altitude, speed_kmh_value, vertical_rate),
        "anomaly_flag": anomaly_flag(on_ground, altitude, speed_kmh_value, vertical_rate),
        "processed_at": unix_to_utc_string(current_time),
    }

    return processed


def process_opensky_response(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Converts raw OpenSky response into a list of processed records.
    """
    current_time = raw_data.get("time")
    states = raw_data.get("states", [])

    processed_records = []
    for state in states:
        if not state or len(state) < 17:
            continue
        processed_records.append(clean_and_transform_state(state, current_time))

    return processed_records