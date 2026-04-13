import unittest

from preprocess import (
    velocity_to_kmh,
    altitude_category,
    stale_data_flag,
    flight_phase,
    normalize_callsign,
)


class TestPreprocess(unittest.TestCase):

    def test_velocity_to_kmh(self):
        self.assertEqual(velocity_to_kmh(100), 360.0)
        self.assertEqual(velocity_to_kmh(None), 0.0)

    def test_altitude_category(self):
        self.assertEqual(altitude_category(0, True), "ground")
        self.assertEqual(altitude_category(1500, False), "low")
        self.assertEqual(altitude_category(5000, False), "medium")
        self.assertEqual(altitude_category(10000, False), "high")

    def test_stale_data_flag(self):
        self.assertTrue(stale_data_flag(100, 80))
        self.assertFalse(stale_data_flag(100, 90))

    def test_flight_phase(self):
        self.assertEqual(flight_phase(True, 0, 0, 0), "parked")
        self.assertEqual(flight_phase(True, 0, 20, 0), "taxi")
        self.assertEqual(flight_phase(False, 500, 250, 3), "takeoff")
        self.assertEqual(flight_phase(False, 5000, 500, 2), "climb")
        self.assertEqual(flight_phase(False, 11000, 800, 0), "cruise")
        self.assertEqual(flight_phase(False, 4000, 500, -3), "descent")
        self.assertEqual(flight_phase(False, 500, 250, -2), "landing")

    def test_normalize_callsign(self):
        self.assertEqual(normalize_callsign(" EIN595 "), "EIN595")
        self.assertEqual(normalize_callsign(None), "UNKNOWN")
        self.assertEqual(normalize_callsign("   "), "UNKNOWN")


if __name__ == "__main__":
    unittest.main()