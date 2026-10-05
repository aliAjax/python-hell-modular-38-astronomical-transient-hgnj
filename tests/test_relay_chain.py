import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.domain import Actor, ConflictError
from src.repository import SQLiteRepository
from src.rules import CADENCE_HOURS, RuleEngine
from src.service import DomainService


class RelayChainTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "chain.db"),
            RuleEngine(),
        )
        self.analyst = Actor("analyst-1", "analyst")
        self.coordinator = Actor("coordinator-1", "coordinator")
        self.supervisor = Actor("supervisor-1", "supervisor")
        source = self.service.create(
            self.analyst, "source", {"name": "Survey", "survey_name": "S"}
        )
        self.source_id = source["id"]
        self.telescope = self.service.create(
            self.coordinator,
            "telescope",
            {"name": "North 2m", "aperture_m": 2.0, "site_name": "NAO"},
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _candidate(self, event_id, transient_type="supernova", magnitude=18.0,
                   observed_at="2026-09-27T00:00:00Z"):
        return self.service.create(
            self.analyst,
            "candidate",
            {
                "source_id": self.source_id,
                "event_id": event_id,
                "ra": 120.5,
                "dec": -12.25,
                "magnitude": magnitude,
                "transient_type": transient_type,
                "observed_at": observed_at,
            },
        )

    def _start_chain(self, candidate, team_id, telescope_id=None, actor=None,
                     transient_type="supernova"):
        return self.service.transition(
            actor or self.coordinator,
            candidate["id"],
            "start_chain",
            {
                "telescope_id": telescope_id or self.telescope["id"],
                "team_id": team_id,
                "mode": "imaging",
            },
        )

    def _segments(self, chain):
        return self.service.get(chain["id"])["data"]["segments"]

    def _observation_for(self, chain, index):
        return self.service.get(chain["data"]["segments"][index]["observation_id"])

    def test_chain_books_cadence_windows_from_measurement_time(self):
        candidate = self._candidate("AT-cadence")
        chain = self._start_chain(candidate, "team-a")
        self.assertEqual(chain["status"], "active")
        segment0 = self._segments(chain)[0]
        self.assertEqual(segment0["state"], "scheduled")
        observation = self._observation_for(chain, 0)
        # Supernova cadence starts 4h after the latest measurement and lasts 0.5h.
        self.assertEqual(observation["data"]["start_at"], "2026-09-27T04:00:00Z")
        self.assertEqual(observation["data"]["end_at"], "2026-09-27T04:30:00Z")
        self.assertEqual(observation["status"], "scheduled")

    def test_completing_a_segment_advances_exactly_one_next_segment(self):
        candidate = self._candidate("AT-advance")
        chain = self._start_chain(candidate, "team-a")
        first = self._observation_for(chain, 0)
        completed = self.service.chains.complete_segment(
            Actor("op-1", "operator"), first["id"], {"quality": "good"}
        )
        self.assertEqual(completed["status"], "completed")
        reloaded = self.service.get(chain["id"])
        states = [item["state"] for item in self._segments(reloaded)]
        self.assertEqual(states[:2], ["completed", "scheduled"])
        self.assertEqual(reloaded["data"]["completed_segments"], 1)
        second = self._observation_for(reloaded, 1)
        self.assertEqual(second["data"]["start_at"], "2026-09-28T00:00:00Z")

    def test_two_actors_completing_the_same_segment_advance_it_once(self):
        candidate = self._candidate("AT-race")
        chain = self._start_chain(candidate, "team-race")
        first = self._observation_for(chain, 0)
        results = []

        def finish(user_id):
            try:
                self.service.chains.complete_segment(
                    Actor(user_id, "operator"), first["id"], {"quality": "good"}
                )
                results.append(("ok", user_id))
            except ConflictError as exc:
                results.append(("conflict", str(exc)))

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(finish, "op-1"), pool.submit(finish, "op-2")]
            for future in futures:
                future.result()

        outcomes = sorted(item[0] for item in results)
        self.assertEqual(outcomes, ["conflict", "ok"])
        reloaded = self.service.get(chain["id"])
        self.assertEqual(reloaded["data"]["completed_segments"], 1)
        self.assertEqual(
            [item["state"] for item in reloaded["data"]["segments"]][:2],
            ["completed", "scheduled"],
        )
        follow_ups = [
            entity
            for entity in self.service.list("observation")
            if entity["data"].get("chain_id") == chain["id"]
        ]
        self.assertEqual(len([o for o in follow_ups if o["data"]["segment_index"] == 1]), 1)

    def test_pending_segments_invalidated_on_reclassify_but_completed_kept(self):
        candidate = self._candidate("AT-reclassify")
        chain = self._start_chain(candidate, "team-a")
        first = self._observation_for(chain, 0)
        self.service.chains.complete_segment(Actor("op-1", "operator"), first["id"], {})
        old_second = self._observation_for(self.service.get(chain["id"]), 1)

        candidate = self.service.transition(
            self.analyst, candidate["id"], "triage", {"reason": "spectrum available"}
        )
        self.service.transition(
            self.supervisor,
            candidate["id"],
            "reclassify",
            {"new_type": "grb", "reason": "gamma ray counterpart"},
        )
        reloaded = self.service.get(chain["id"])
        states = [item["state"] for item in reloaded["data"]["segments"]]
        self.assertEqual(states[0], "completed")
        self.assertEqual(len(states), len(CADENCE_HOURS["grb"][0]))
        self.assertIn("scheduled", states)
        # The old supernova second-segment booking was withdrawn; completed one stays.
        self.assertEqual(self.service.get(old_second["id"])["status"], "withdrawn")
        self.assertEqual(self.service.get(first["id"])["status"], "completed")
        # GRB cadence second segment is 1h after the anchor.
        new_second = self._observation_for(reloaded, 1)
        self.assertEqual(new_second["data"]["start_at"], "2026-09-27T01:00:00Z")

    def test_fade_over_two_magnitudes_replans_remaining_segments(self):
        candidate = self._candidate("AT-fade")
        chain = self._start_chain(candidate, "team-fade")
        first = self._observation_for(chain, 0)
        self.service.chains.complete_segment(Actor("op-1", "operator"), first["id"], {})
        reloaded = self.service.get(chain["id"])
        booked_second = self._observation_for(reloaded, 1)

        candidate = self.service.transition(
            self.analyst,
            candidate["id"],
            "merge_measurement",
            {
                "measurement": {
                    "observed_at": "2026-09-30T00:00:00Z",
                    "ra": 120.5,
                    "dec": -12.25,
                    "magnitude": 20.1,
                }
            },
        )
        reloaded = self.service.get(chain["id"])
        self.assertEqual(self.service.get(booked_second["id"])["status"], "withdrawn")
        self.assertEqual(self.service.get(first["id"])["status"], "completed")
        self.assertEqual(reloaded["data"]["anchor_at"], "2026-09-30T00:00:00Z")
        self.assertEqual(reloaded["data"]["segments"][0]["state"], "completed")
        self.assertEqual(reloaded["data"]["segments"][1]["state"], "scheduled")
        replacement = self._observation_for(reloaded, 1)
        # 4h supernova cadence after the new anchor; booked window is at least that.
        self.assertGreaterEqual(
            replacement["data"]["start_at"], "2026-09-30T04:00:00Z"
        )

    def test_small_brightness_change_does_not_replan(self):
        candidate = self._candidate("AT-bright")
        chain = self._start_chain(candidate, "team-bright")
        first = self._observation_for(chain, 0)
        self.service.chains.complete_segment(Actor("op-1", "operator"), first["id"], {})
        reloaded = self.service.get(chain["id"])
        booked_second = self._observation_for(reloaded, 1)

        self.service.transition(
            self.analyst,
            candidate["id"],
            "merge_measurement",
            {
                "measurement": {
                    "observed_at": "2026-09-27T06:00:00Z",
                    "ra": 120.5,
                    "dec": -12.25,
                    "magnitude": 19.9,
                }
            },
        )
        still = self.service.get(chain["id"])
        self.assertEqual(still["data"]["segments"][1]["state"], "scheduled")
        self.assertEqual(
            still["data"]["segments"][1]["observation_id"], booked_second["id"]
        )

    def test_withdraw_terminates_chain_and_keeps_completed_history(self):
        candidate = self._candidate("AT-withdraw")
        chain = self._start_chain(candidate, "team-a")
        first = self._observation_for(chain, 0)
        self.service.chains.complete_segment(Actor("op-1", "operator"), first["id"], {})
        pending = self._observation_for(self.service.get(chain["id"]), 1)

        candidate = self.service.transition(
            self.analyst, candidate["id"], "triage", {"reason": "review"}
        )
        self.service.transition(
            self.supervisor, candidate["id"], "withdraw", {"reason": "artifacts"}
        )
        reloaded = self.service.get(chain["id"])
        self.assertEqual(reloaded["status"], "terminated")
        self.assertEqual(reloaded["data"]["segments"][0]["state"], "completed")
        self.assertTrue(
            all(item["state"] == "invalidated" for item in reloaded["data"]["segments"][1:])
        )
        self.assertEqual(self.service.get(first["id"])["status"], "completed")
        self.assertEqual(self.service.get(pending["id"])["status"], "withdrawn")

    def test_failed_next_segment_keeps_progress_and_retry_books_once(self):
        candidate = self._candidate("AT-fail", transient_type="grb")
        chain = self._start_chain(candidate, "team-fail")
        first = self._observation_for(chain, 0)
        # Make the telescope unusable before completion, so successor planning fails.
        self.service.transition(
            self.coordinator, self.telescope["id"], "restrict", {"reason": "maintenance"}
        )
        self.service.chains.complete_segment(Actor("op-1", "operator"), first["id"], {})
        reloaded = self.service.get(chain["id"])
        segment1 = reloaded["data"]["segments"][1]
        self.assertEqual(first and self.service.get(first["id"])["status"], "completed")
        self.assertEqual(segment1["state"], "failed")
        self.assertEqual(segment1["attempts"], 1)
        self.assertTrue(segment1["failure_reason"])
        self.assertIsNone(segment1.get("observation_id"))
        self.assertEqual(reloaded["status"], "active")

        self.service.transition(
            self.coordinator, self.telescope["id"], "restore", {}
        )
        reloaded = self.service.transition(
            self.coordinator, reloaded["id"], "retry_segment", {}
        )
        segment1 = reloaded["data"]["segments"][1]
        self.assertEqual(segment1["state"], "scheduled")
        self.assertEqual(segment1["attempts"], 2)
        booked = self.service.get(segment1["observation_id"])
        self.assertEqual(booked["status"], "scheduled")
        # A second retry on the now-scheduled segment is rejected.
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.coordinator, reloaded["id"], "retry_segment", {}
            )

    def test_observation_write_failure_does_not_occupy_a_slot(self):
        candidate = self._candidate("AT-writefail", transient_type="grb")
        chain = self._start_chain(candidate, "team-write")
        first = self._observation_for(chain, 0)
        original = self.service.repository._insert_entity
        calls = {"count": 0}

        def flaky(connection, entity_id, kind, status, data, actor_id):
            if kind == "observation":
                calls["count"] += 1
                raise RuntimeError("simulated storage outage")
            return original(connection, entity_id, kind, status, data, actor_id)

        self.service.repository._insert_entity = flaky
        try:
            self.service.chains.complete_segment(Actor("op-1", "operator"), first["id"], {})
        finally:
            self.service.repository._insert_entity = original
        reloaded = self.service.get(chain["id"])
        self.assertEqual(self.service.get(first["id"])["status"], "completed")
        self.assertEqual(reloaded["data"]["segments"][1]["state"], "failed")
        self.assertIn("simulated storage outage",
                      reloaded["data"]["segments"][1]["failure_reason"])
        # Retrying after recovery books exactly one observation.
        reloaded = self.service.transition(
            self.coordinator, chain["id"], "retry_segment", {}
        )
        self.assertEqual(reloaded["data"]["segments"][1]["state"], "scheduled")
        successors = [
            entity
            for entity in self.service.list("observation")
            if entity["data"].get("chain_id") == chain["id"]
            and entity["data"]["segment_index"] == 1
        ]
        self.assertEqual(len(successors), 1)

    def test_manually_withdrawn_booking_can_be_retried_without_double_booking(self):
        candidate = self._candidate("AT-unbook", transient_type="grb")
        chain = self._start_chain(candidate, "team-unbook")
        first = self._observation_for(chain, 0)
        self.service.chains.complete_segment(Actor("op-1", "operator"), first["id"], {})
        reloaded = self.service.get(chain["id"])
        booked = self._observation_for(reloaded, 1)

        reloaded = self.service.transition(
            Actor("admin-1", "admin"), booked["id"], "withdraw", {"reason": "weather"}
        )
        self.assertEqual(reloaded["status"], "withdrawn")
        chain_state = self.service.get(chain["id"])
        segment1 = chain_state["data"]["segments"][1]
        self.assertEqual(segment1["state"], "failed")
        self.assertIn("weather", segment1["failure_reason"])
        self.assertIsNone(segment1.get("observation_id"))

        chain_state = self.service.transition(
            self.coordinator, chain["id"], "retry_segment", {}
        )
        segment1 = chain_state["data"]["segments"][1]
        self.assertEqual(segment1["state"], "scheduled")
        replacement = self.service.get(segment1["observation_id"])
        self.assertEqual(replacement["status"], "scheduled")
        self.assertNotEqual(replacement["id"], booked["id"])
        segment_one = [
            entity
            for entity in self.service.list("observation")
            if entity["data"].get("chain_id") == chain["id"]
            and entity["data"]["segment_index"] == 1
            and entity["status"] == "scheduled"
        ]
        self.assertEqual(len(segment_one), 1)


if __name__ == "__main__":
    unittest.main()
