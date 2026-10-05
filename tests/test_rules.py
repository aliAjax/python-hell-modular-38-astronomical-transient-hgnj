import unittest

from src.domain import ConflictError, ValidationError
from src.rules import (
    RuleEngine,
    calculate_priority,
    find_free_window,
    is_faded,
    magnitude_fade,
    measurements_overlap,
    nominal_window,
)


class RulesTest(unittest.TestCase):
    def test_priority_prefers_bright_and_high_value_events(self):
        self.assertGreater(calculate_priority(16.0, "grb"), calculate_priority(19.0, "variable"))
        with self.assertRaises(ValidationError):
            calculate_priority("not-a-number", "unknown")

    def test_overlap_is_half_open(self):
        self.assertTrue(measurements_overlap("10", "12", "11", "13"))
        self.assertFalse(measurements_overlap("10", "11", "11", "12"))

    def test_nominal_window_anchors_on_measurement_time(self):
        start, end = nominal_window(
            "grb", 0, "2026-09-27T00:00:00Z"
        )
        self.assertEqual(start, "2026-09-27T00:10:00Z")
        self.assertEqual(end, "2026-09-27T00:25:00Z")
        start, _ = nominal_window("supernova", 1, "2026-09-27T00:00:00Z")
        self.assertEqual(start, "2026-09-28T00:00:00Z")

    def test_free_window_slides_past_busy_periods_but_never_earlier(self):
        # The telescope/team is busy exactly during the nominal slot.
        chosen = find_free_window(
            "2026-09-27T04:00:00Z",
            "2026-09-27T04:30:00Z",
            [("2026-09-27T04:05:00Z", "2026-09-27T05:00:00Z")],
        )
        self.assertEqual(chosen, ("2026-09-27T05:00:00Z", "2026-09-27T05:30:00Z"))
        # Back-to-back windows do not collide (half-open).
        chosen = find_free_window(
            "2026-09-27T04:00:00Z",
            "2026-09-27T04:30:00Z",
            [("2026-09-27T03:30:00Z", "2026-09-27T04:00:00Z")],
        )
        self.assertEqual(chosen[0], "2026-09-27T04:00:00Z")
        # No answer inside the horizon when the resource stays booked.
        missing = find_free_window(
            "2026-09-27T04:00:00Z",
            "2026-09-27T04:30:00Z",
            [("2026-09-27T04:00:00Z", "2026-09-30T00:00:00Z")],
        )
        self.assertIsNone(missing)

    def test_fade_detection_uses_magnitude_delta(self):
        self.assertTrue(is_faded(18.0, 20.01))
        self.assertFalse(is_faded(18.0, 20.0))
        self.assertFalse(is_faded(18.0, 17.0))
        self.assertGreater(magnitude_fade(18.0, 20.5), 2.0)

    def test_duplicate_event_and_measurement_are_rejected(self):
        rules = RuleEngine()
        candidate = {
            "id": "candidate-1",
            "kind": "candidate",
            "status": "detected",
            "data": {"measurements": [{"observed_at": "2026-09-27T01:00:00Z"}]},
        }
        with self.assertRaises(ConflictError):
            rules.validate_transition(
                type("Actor", (), {"role": "analyst", "user_id": "a"})(),
                candidate,
                "merge_measurement",
                {
                    "measurement": {
                        "observed_at": "2026-09-27T01:00:00Z",
                        "ra": 1,
                        "dec": 2,
                        "magnitude": 18,
                    }
                },
            )


if __name__ == "__main__":
    unittest.main()
