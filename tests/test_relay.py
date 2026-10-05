import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from src.domain import Actor, ConflictError, InvalidTransition
from src.repository import SQLiteRepository
from src.rules import RELAY_HORIZON_DAYS, _format_iso, _parse_iso
from src.service import DomainService


class RelayTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "relay.db"),
        )
        self.analyst = Actor("analyst-1", "analyst")
        self.coordinator = Actor("coordinator-1", "coordinator")
        self.operator = Actor("operator-1", "operator")

    def tearDown(self):
        self.tmp.cleanup()

    def _setup_relay(self, transient_type="supernova", observed_at="2026-09-27T01:00:00Z"):
        source = self.service.create(
            self.analyst, "source", {"name": "Survey", "survey_name": "S"}
        )
        candidate = self.service.create(
            self.analyst,
            "candidate",
            {
                "source_id": source["id"],
                "event_id": "AT-1",
                "ra": 10,
                "dec": 20,
                "magnitude": 17,
                "transient_type": transient_type,
                "observed_at": observed_at,
            },
        )
        telescope = self.service.create(
            self.coordinator,
            "telescope",
            {"name": "T1", "aperture_m": 2.0, "site_name": "S1"},
        )
        obs1 = self.service.create(
            self.coordinator,
            "observation",
            {
                "candidate_id": candidate["id"],
                "telescope_id": telescope["id"],
                "team_id": "team-1",
                "start_at": "2026-09-28T10:00:00Z",
                "end_at": "2026-09-28T11:00:00Z",
                "mode": "imaging",
                "segment_no": 1,
            },
        )
        self.service.transition(
            self.coordinator, obs1["id"], "schedule", {"operator_id": "op-1"}
        )
        return candidate, telescope, obs1

    def _segments(self, candidate_id, include_withdrawn=False):
        items = []
        for obs in self.service.list("observation"):
            data = obs["data"]
            if data.get("candidate_id") != candidate_id:
                continue
            if data.get("segment_no") is None:
                continue
            if not include_withdrawn and obs["status"] == "withdrawn":
                continue
            items.append(obs)
        return sorted(items, key=lambda o: int(o["data"]["segment_no"]))

    def test_complete_advances_to_next_segment(self):
        candidate, telescope, obs1 = self._setup_relay()
        completed = self.service.transition(
            self.operator, obs1["id"], "complete", {"quality": "good"}
        )
        self.assertEqual(completed["status"], "completed")

        segments = self._segments(candidate["id"])
        self.assertEqual([int(s["data"]["segment_no"]) for s in segments], [1, 2])
        seg2 = segments[1]
        self.assertEqual(seg2["status"], "requested")
        self.assertEqual(seg2["data"]["telescope_id"], telescope["id"])
        self.assertEqual(seg2["data"]["team_id"], "team-1")
        # supernova segment 2 is ~2 days after the measurement instant.
        self.assertEqual(seg2["data"]["start_at"], "2026-09-29T01:00:00Z")

        relay = self.service.get(candidate["id"])["data"]["relay"]
        self.assertEqual(relay["status"], "active")
        self.assertEqual(relay["last_completed"], 1)
        self.assertIsNone(relay["failure"])

    def test_cadence_depends_on_event_type(self):
        candidate, _, obs1 = self._setup_relay(transient_type="grb")
        self.service.transition(self.operator, obs1["id"], "complete", {"quality": "good"})
        seg2 = self._segments(candidate["id"])[1]
        # grb segment 2 is ~1 hour after the measurement instant.
        self.assertEqual(seg2["data"]["start_at"], "2026-09-27T02:00:00Z")

    def test_free_window_slides_past_occupancy(self):
        candidate, telescope, obs1 = self._setup_relay()
        # A different candidate occupies the ideal window of segment 2 for
        # both the telescope and the team.
        source2 = self.service.create(
            self.analyst, "source", {"name": "Survey2", "survey_name": "S2"}
        )
        candidate2 = self.service.create(
            self.analyst,
            "candidate",
            {
                "source_id": source2["id"],
                "event_id": "AT-2",
                "ra": 11,
                "dec": 21,
                "magnitude": 18,
                "transient_type": "unknown",
                "observed_at": "2026-09-27T01:00:00Z",
            },
        )
        block = self.service.create(
            self.coordinator,
            "observation",
            {
                "candidate_id": candidate2["id"],
                "telescope_id": telescope["id"],
                "team_id": "team-1",
                "start_at": "2026-09-29T00:00:00Z",
                "end_at": "2026-09-29T02:00:00Z",
                "mode": "imaging",
            },
        )
        self.service.transition(
            self.coordinator, block["id"], "schedule", {"operator_id": "op-1"}
        )
        self.service.transition(self.operator, obs1["id"], "complete", {"quality": "good"})
        seg2 = self._segments(candidate["id"])[1]
        # Slides to the end of the occupied interval.
        self.assertEqual(seg2["data"]["start_at"], "2026-09-29T02:00:00Z")

    def test_advance_is_idempotent_no_double_occupancy(self):
        candidate, _, obs1 = self._setup_relay()
        self.service.transition(self.operator, obs1["id"], "complete", {"quality": "good"})
        first = self._segments(candidate["id"])[1]
        # Retrying advance (e.g. the response was lost) returns the same segment.
        retried = self.service.transition(
            self.coordinator, candidate["id"], "advance_relay"
        )
        self.assertEqual(retried["id"], first["id"])
        self.assertEqual(len(self._segments(candidate["id"])), 2)

    def test_concurrent_complete_only_advances_once(self):
        candidate, _, obs1 = self._setup_relay()
        self.service.transition(
            self.operator, obs1["id"], "complete", {"quality": "good"}
        )
        with self.assertRaises((ConflictError, InvalidTransition)):
            self.service.transition(
                self.operator,
                obs1["id"],
                "complete",
                {"quality": "good"},
                expected_version=obs1["version"],
            )
        segments = self._segments(candidate["id"])
        self.assertEqual(len(segments), 2)
        self.assertEqual(segments[1]["data"]["segment_no"], 2)

    def test_dimming_beyond_two_mags_recomputes(self):
        candidate, _, obs1 = self._setup_relay()
        self.service.transition(self.operator, obs1["id"], "complete", {"quality": "good"})
        self.assertEqual(self._segments(candidate["id"])[1]["status"], "requested")

        # Merge a measurement 2.5 magnitudes fainter than the relay base.
        self.service.transition(
            self.analyst,
            candidate["id"],
            "merge_measurement",
            {
                "measurement": {
                    "observed_at": "2026-09-30T01:00:00Z",
                    "ra": 10.1,
                    "dec": 20.1,
                    "magnitude": 19.5,
                }
            },
        )

        segments = self._segments(candidate["id"])
        # Completed segment 1 is kept; the pending segment 2 was withdrawn and
        # recomputed from the new measurement instant.
        self.assertEqual([int(s["data"]["segment_no"]) for s in segments], [1, 2])
        self.assertEqual(segments[0]["status"], "completed")
        self.assertEqual(segments[1]["status"], "requested")
        self.assertEqual(segments[1]["data"]["start_at"], "2026-10-02T01:00:00Z")
        relay = self.service.get(candidate["id"])["data"]["relay"]
        self.assertEqual(relay["base_observed_at"], "2026-09-30T01:00:00Z")
        self.assertEqual(relay["base_magnitude"], 19.5)
        self.assertIsNone(relay["failure"])

    def test_reclassify_recomputes_with_new_cadence(self):
        candidate, _, obs1 = self._setup_relay()
        self.service.transition(self.operator, obs1["id"], "complete", {"quality": "good"})
        self.service.transition(
            self.analyst, candidate["id"], "triage", {"reason": "review"}
        )
        self.service.transition(
            self.analyst,
            candidate["id"],
            "reclassify",
            {"new_type": "grb", "reason": "revised classification"},
        )
        seg2 = self._segments(candidate["id"])[1]
        self.assertEqual(seg2["data"]["start_at"], "2026-09-27T02:00:00Z")
        relay = self.service.get(candidate["id"])["data"]["relay"]
        self.assertEqual(relay["reason"], "reclassified")

    def test_withdraw_invalidates_relay(self):
        candidate, _, obs1 = self._setup_relay()
        self.service.transition(self.operator, obs1["id"], "complete", {"quality": "good"})
        self.service.transition(
            self.analyst, candidate["id"], "withdraw", {"reason": "bogus event"}
        )
        relay = self.service.get(candidate["id"])["data"]["relay"]
        self.assertEqual(relay["status"], "invalidated")
        self.assertEqual(relay["reason"], "withdrawn")
        self.assertEqual(
            self._segments(candidate["id"], include_withdrawn=True)[1]["status"],
            "withdrawn",
        )
        with self.assertRaises(InvalidTransition):
            self.service.transition(self.coordinator, candidate["id"], "advance_relay")

    def test_failure_recorded_and_resume_does_not_reoccupy(self):
        candidate, telescope, obs1 = self._setup_relay()
        # Tile the whole horizon with scheduled observations (for a different
        # candidate) so the telescope/team have no free slot.
        source2 = self.service.create(
            self.analyst, "source", {"name": "Survey2", "survey_name": "S2"}
        )
        candidate2 = self.service.create(
            self.analyst,
            "candidate",
            {
                "source_id": source2["id"],
                "event_id": "AT-2",
                "ra": 11,
                "dec": 21,
                "magnitude": 18,
                "transient_type": "unknown",
                "observed_at": "2026-09-27T01:00:00Z",
            },
        )
        start = _parse_iso("2026-09-29T01:00:00Z")
        blockers = []
        for i in range(RELAY_HORIZON_DAYS):
            day_start = _format_iso(start + timedelta(days=i))
            day_end = _format_iso(start + timedelta(days=i + 1))
            block = self.service.create(
                self.coordinator,
                "observation",
                {
                    "candidate_id": candidate2["id"],
                    "telescope_id": telescope["id"],
                    "team_id": "team-1",
                    "start_at": day_start,
                    "end_at": day_end,
                    "mode": "imaging",
                },
            )
            self.service.transition(
                self.coordinator, block["id"], "schedule", {"operator_id": "op-1"}
            )
            blockers.append(block)

        self.service.transition(self.operator, obs1["id"], "complete", {"quality": "good"})
        relay = self.service.get(candidate["id"])["data"]["relay"]
        self.assertIsNotNone(relay["failure"])
        self.assertEqual(relay["failure"]["segment_no"], 2)
        self.assertIn("no free window", relay["failure"]["reason"])
        self.assertEqual(len(self._segments(candidate["id"])), 1)

        # Free one slot and resume; only one segment 2 is created.
        self.service.transition(
            self.analyst, blockers[0]["id"], "withdraw", {"reason": "freed"}
        )
        resumed = self.service.transition(
            self.coordinator, candidate["id"], "advance_relay"
        )
        self.assertEqual(resumed["data"]["segment_no"], 2)
        self.assertEqual(len(self._segments(candidate["id"])), 2)
        relay = self.service.get(candidate["id"])["data"]["relay"]
        self.assertIsNone(relay["failure"])


if __name__ == "__main__":
    unittest.main()
