import unittest
from unittest.mock import patch, MagicMock

from app import create_app


MOCK_RAW_DATA = {
    "time": 1776034548,
    "states": [
        [
            "4ca293",        # icao24
            "EIN595  ",      # callsign
            "Ireland",       # origin_country
            1776034532,      # time_position
            1776034532,      # last_contact
            -6.2801,         # longitude
            53.4236,         # latitude
            None,            # baro_altitude
            True,            # on_ground
            5.4,             # velocity
            92.81,           # heading
            0.0,             # vertical_rate
            None,            # sensors
            None,            # geo_altitude
            "6013",          # squawk
            False,           # spi
            0                # position_source
        ]
    ]
}


class TestPipelineIntegration(unittest.TestCase):

    def setUp(self):
        self.app = create_app().test_client()
        self.app.testing = True

    @patch("pipeline.fetch_opensky_data")
    @patch("pipeline.raw_collection")
    @patch("pipeline.processed_collection")
    def test_run_pipeline_route(self, mock_processed_collection, mock_raw_collection, mock_fetch):
        mock_fetch.return_value = MOCK_RAW_DATA

        mock_raw_insert = MagicMock()
        mock_raw_insert.inserted_id = "raw123"
        mock_raw_collection.insert_one.return_value = mock_raw_insert

        mock_processed_insert = MagicMock()
        mock_processed_insert.inserted_ids = ["proc1"]
        mock_processed_collection.insert_many.return_value = mock_processed_insert

        response = self.app.get("/run-pipeline")
        self.assertEqual(response.status_code, 200)

        data = response.get_json()
        self.assertEqual(data["message"], "Pipeline executed successfully")
        self.assertEqual(data["raw_records_count"], 1)
        self.assertEqual(data["processed_records_count"], 1)


if __name__ == "__main__":
    unittest.main()
    