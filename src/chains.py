"""Relay-chain orchestration for transient follow-up.

A chain ties a candidate to one telescope and one observing team and lays the
event out as a cadence of segments.  Every segment is its own observation
application ("各段观测单独建单"); when a segment completes, the next one is
created from the latest measurement time and the type-specific cadence.

The orchestration never opens its own transaction.  Every ``*_tx`` method runs
inside a caller-provided write transaction so that marking a segment completed
and booking its successor commit atomically.  A failure while writing the
successor therefore cannot consume a slot: it is recorded on the segment
(``failed`` + reason + attempt count) and ``retry_segment`` tries again without
re-using or double-booking anything.

Segment array contract: the index in ``segments`` is the cadence index of the
current plan.  Completed segments always keep their position.  When the chain
is recalculated, superseded non-completed segments move to
``invalidated_history`` (their observation applications are withdrawn) and the
tail is rebuilt with fresh ``pending`` slots.
"""
from uuid import uuid4

from .domain import ConflictError, InvalidTransition, NotFoundError, ValidationError
from .repository import utcnow
from .rules import (
    cadence_for,
    find_free_window,
    is_faded,
    nominal_window,
)

ACTIVE_CHAIN_STATUSES = ("active",)
PENDING_STATES = ("scheduled", "failed", "pending")


class RelayChainManager:
    def __init__(self, repository, rules):
        self.repository = repository
        self.rules = rules

    # -- low-level helpers ----------------------------------------------------

    def _audit(self, connection, entity_id, actor, action, from_status, to_status, detail=None):
        self.repository._append_audit(
            connection, entity_id, actor.user_id, actor.role,
            action, from_status, to_status, detail or {},
        )

    @staticmethod
    def _latest_anchor(candidate):
        measurements = sorted(
            candidate["data"].get("measurements", []),
            key=lambda item: item["observed_at"],
        )
        return measurements[-1]["observed_at"] if measurements else candidate["data"]["observed_at"]

    @staticmethod
    def _persist(connection, repository, chain, status=None):
        """Write the in-memory chain back, keeping its optimistic version."""
        data = chain["data"]
        data["completed_segments"] = sum(
            1 for item in data.get("segments", []) if item.get("state") == "completed"
        )
        data["pending_index"] = next(
            (i for i, item in enumerate(data.get("segments", [])) if item.get("state") in PENDING_STATES),
            len(data.get("segments", [])),
        )
        repository._update_entity(
            connection, chain["id"], chain["version"], status or chain["status"], data
        )
        chain["version"] = int(chain["version"]) + 1
        if status:
            chain["status"] = status

    @staticmethod
    def _active_chain_tx(connection, repository, candidate_id):
        for chain in repository._find_entities(connection, "chain", "candidate_id", candidate_id):
            if chain["status"] in ACTIVE_CHAIN_STATUSES:
                return chain
        return None

    def _chain_for_observation_tx(self, connection, observation):
        chain_id = observation["data"].get("chain_id")
        if not chain_id:
            return None
        chain = self.repository._load_entity(connection, chain_id)
        if not chain or chain["kind"] != "chain":
            raise ConflictError("observation references missing chain: " + chain_id)
        return chain

    def _busy_windows_tx(self, connection, telescope_id, team_id):
        windows = []
        for entity in self.repository._list_entities(connection, kind="observation", status="scheduled"):
            data = entity["data"]
            if data.get("telescope_id") == telescope_id or data.get("team_id") == team_id:
                windows.append((data.get("start_at"), data.get("end_at")))
        return windows

    def _finish_if_complete(self, connection, chain, actor):
        if chain["status"] != "active":
            return
        if chain["data"]["pending_index"] >= len(chain["data"]["segments"]):
            self._persist(connection, self.repository, chain, status="completed")
            self._audit(connection, chain["id"], actor, "chain_completed",
                        "active", "completed",
                        {"segments": len(chain["data"]["segments"])})

    def _plan_next_tx(self, connection, chain, actor, reason):
        """Book the pending segment. Failures are recorded, never raised.

        Returns the new observation id on success, otherwise None (the segment
        is left in ``failed`` with its reason and attempt count).
        """
        data = chain["data"]
        index = data["pending_index"]
        transient_type = data["transient_type"]
        delays, _ = cadence_for(transient_type)
        if index >= len(delays):
            self._finish_if_complete(connection, chain, actor)
            return None
        segment = data["segments"][index]

        def fail(message):
            segment["state"] = "failed"
            segment["attempts"] = int(segment.get("attempts", 0)) + 1
            segment["failure_reason"] = message
            segment["failed_at"] = utcnow()
            self._persist(connection, self.repository, chain)
            self._audit(connection, chain["id"], actor, "segment_failed",
                        chain["status"], chain["status"],
                        {"segment_index": index, "attempts": segment["attempts"],
                         "reason": reason, "message": message})
            return None

        telescope = self.repository._load_entity(connection, data["telescope_id"])
        if not telescope or telescope["status"] != "available":
            return fail("telescope is unavailable")
        candidate = self.repository._load_entity(connection, data["candidate_id"])
        if not candidate or candidate["status"] == "withdrawn":
            return fail("candidate is unavailable")
        try:
            nominal = nominal_window(transient_type, index, data["anchor_at"],
                                     data.get("duration_hours"))
        except ValidationError as exc:
            return fail(str(exc))
        busy = self._busy_windows_tx(connection, data["telescope_id"], data["team_id"])
        chosen = find_free_window(nominal[0], nominal[1], busy)
        if chosen is None:
            return fail("no free window within search horizon")
        observation_id = str(uuid4())
        observation_data = {
            "candidate_id": data["candidate_id"],
            "telescope_id": data["telescope_id"],
            "team_id": data["team_id"],
            "mode": data.get("mode", "imaging"),
            "start_at": chosen[0],
            "end_at": chosen[1],
            "chain_id": chain["id"],
            "segment_index": index,
            "nominal_start_at": nominal[0],
            "nominal_end_at": nominal[1],
            "scheduled_team": data["team_id"],
            "scheduled_by": actor.user_id,
        }
        try:
            self.repository._insert_entity(
                connection, observation_id, "observation", "scheduled",
                observation_data, actor.user_id,
            )
        except Exception as exc:
            # The booking was not persisted; nothing is occupied. Preserve the
            # progress already committed in this transaction.
            return fail("observation write failed: " + str(exc))
        segment.update({
            "state": "scheduled",
            "observation_id": observation_id,
            "window_start_at": chosen[0],
            "window_end_at": chosen[1],
            "attempts": int(segment.get("attempts", 0)) + 1,
            "failure_reason": None,
        })
        self._persist(connection, self.repository, chain)
        self._audit(connection, observation_id, actor, "chain_schedule", None, "scheduled",
                    {"chain_id": chain["id"], "segment_index": index, "reason": reason})
        return observation_id

    # -- public entry points --------------------------------------------------

    def start(self, actor, candidate_id, payload, expected_version=None):
        self.rules.ensure_chain_role(actor, "start_chain")
        for field in ("telescope_id", "team_id"):
            if not str(payload.get(field, "")).strip():
                raise ValidationError("missing required field: " + field)
        telescope_id = payload["telescope_id"]
        team_id = payload["team_id"]
        duration = payload.get("duration_hours")
        if duration is not None:
            try:
                duration = float(duration)
            except (TypeError, ValueError):
                raise ValidationError("duration_hours must be numeric")
            if duration <= 0:
                raise ValidationError("duration_hours must be positive")
        with self.repository.transaction() as connection:
            candidate = self.repository._load_entity(connection, candidate_id)
            if not candidate or candidate["kind"] != "candidate":
                raise NotFoundError("candidate not found: " + candidate_id)
            if candidate["status"] == "withdrawn":
                raise InvalidTransition("cannot start a chain on a withdrawn candidate")
            if expected_version is not None and candidate["version"] != int(expected_version):
                raise ConflictError("version conflict for candidate")
            telescope = self.repository._load_entity(connection, telescope_id)
            if not telescope or telescope["kind"] != "telescope":
                raise ValidationError("telescope does not exist")
            if self._active_chain_tx(connection, self.repository, candidate_id):
                raise ConflictError("candidate already has an active relay chain")
            transient_type = candidate["data"].get("transient_type")
            delays, default_duration = cadence_for(transient_type)
            chain_id = str(uuid4())
            chain_data = {
                "candidate_id": candidate_id,
                "telescope_id": telescope_id,
                "team_id": team_id,
                "mode": payload.get("mode", "imaging"),
                "transient_type": transient_type,
                "anchor_at": self._latest_anchor(candidate),
                "duration_hours": duration if duration is not None else default_duration,
                "cadence_hours": delays,
                "segments": [
                    {"state": "pending", "observation_id": None, "attempts": 0}
                    for _ in delays
                ],
                "invalidated_history": [],
                "completed_segments": 0,
                "pending_index": 0,
            }
            self.repository._insert_entity(
                connection, chain_id, "chain", "active", chain_data, actor.user_id
            )
            self._audit(connection, candidate_id, actor, "start_chain",
                        candidate["status"], candidate["status"], {"chain_id": chain_id})
            chain = self.repository._load_entity(connection, chain_id)
            self._plan_next_tx(connection, chain, actor, "chain_start")
            return self.repository._load_entity(connection, chain_id)

    def complete_segment(self, actor, observation_id, data=None, expected_version=None):
        self.rules.ensure_chain_role(actor, "complete_segment")
        with self.repository.transaction() as connection:
            observation = self.repository._load_entity(connection, observation_id)
            if not observation or observation["kind"] != "observation":
                raise NotFoundError("observation not found: " + observation_id)
            chain = self._chain_for_observation_tx(connection, observation)
            if observation["status"] == "completed":
                # A second completion submission (two racing actors included)
                # must not advance the chain a second time.
                raise ConflictError("observation segment already completed")
            if observation["status"] != "scheduled":
                raise InvalidTransition(
                    "cannot complete observation from status " + observation["status"]
                )
            if expected_version is not None and observation["version"] != int(expected_version):
                raise ConflictError("version conflict for observation")
            index = observation["data"]["segment_index"]
            completed_data = dict(observation["data"])
            completed_data.update(dict(data or {}))
            completed_data["completed_by"] = actor.user_id
            self.repository._update_entity(
                connection, observation_id, observation["version"], "completed", completed_data
            )
            self._audit(connection, observation_id, actor, "complete", "scheduled", "completed",
                        {"chain_id": chain["id"], "segment_index": index})
            chain["data"]["segments"][index]["state"] = "completed"
            chain["data"]["segments"][index]["failure_reason"] = None
            # Next window is measured from the most recent measurement.
            candidate = self.repository._load_entity(connection, chain["data"]["candidate_id"])
            chain["data"]["anchor_at"] = self._latest_anchor(candidate)
            self._persist(connection, self.repository, chain)
            self._plan_next_tx(connection, chain, actor, "previous_segment_completed")
            return self.repository._load_entity(connection, observation_id)

    def retry_segment(self, actor, chain_id, segment_index=None, expected_version=None):
        self.rules.ensure_chain_role(actor, "retry_segment")
        with self.repository.transaction() as connection:
            chain = self.repository._load_entity(connection, chain_id)
            if not chain or chain["kind"] != "chain":
                raise NotFoundError("chain not found: " + chain_id)
            if chain["status"] not in ACTIVE_CHAIN_STATUSES:
                raise InvalidTransition("chain is not active: " + chain["status"])
            if expected_version is not None and chain["version"] != int(expected_version):
                raise ConflictError("version conflict for chain")
            index = chain["data"]["pending_index"] if segment_index is None else int(segment_index)
            segments = chain["data"]["segments"]
            if index >= len(segments):
                raise ValidationError("segment index outside cadence")
            if segments[index].get("state") != "failed":
                raise ConflictError(
                    "segment %s is not failed (state=%s)" % (index, segments[index].get("state"))
                )
            self._audit(connection, chain["id"], actor, "retry_segment",
                        chain["status"], chain["status"], {"segment_index": index})
            # A failed segment never created an observation, so retrying cannot
            # double-occupy a window.
            self._plan_next_tx(connection, chain, actor, "manual_retry")
            return self.repository._load_entity(connection, chain_id)

    # -- candidate hooks (called inside the candidate's transaction) ---------

    def on_observation_withdraw(self, connection, observation, actor, reason):
        """A booked segment was withdrawn manually; park the segment as failed.

        The completed history is untouched, and ``retry_segment`` can book a
        replacement window without double-occupying anything.
        """
        chain = self._chain_for_observation_tx(connection, observation)
        if not chain or chain["status"] not in ACTIVE_CHAIN_STATUSES:
            return
        index = observation["data"].get("segment_index")
        if index is None or index >= len(chain["data"]["segments"]):
            return
        segment = chain["data"]["segments"][index]
        if segment.get("state") in ("completed", "invalidated"):
            return
        segment.update({
            "state": "failed",
            "attempts": int(segment.get("attempts", 0)),
            "failure_reason": "observation withdrawn: " + str(reason or "unspecified"),
            "failed_at": utcnow(),
            "observation_id": None,
        })
        self._persist(connection, self.repository, chain)
        self._audit(connection, chain["id"], actor, "segment_unbooked",
                    chain["status"], chain["status"],
                    {"segment_index": index, "reason": reason})

    def on_candidate_withdraw(self, connection, candidate, actor, reason):
        chain = self._active_chain_tx(connection, self.repository, candidate["id"])
        if not chain:
            return
        self._invalidate_pending(connection, chain, actor, "candidate_withdrawn", reason)
        self._persist(connection, self.repository, chain, status="terminated")
        self._audit(connection, chain["id"], actor, "chain_terminated",
                    "active", "terminated", {"reason": reason})

    def on_candidate_reclassify(self, connection, candidate, actor, new_type, reason):
        chain = self._active_chain_tx(connection, self.repository, candidate["id"])
        if not chain:
            return
        self._rebuild_cadence(connection, chain, candidate, actor, new_type,
                              "candidate_reclassified", {"new_type": new_type, "reason": reason})

    def on_candidate_measurement(self, connection, candidate, actor, before, after):
        chain = self._active_chain_tx(connection, self.repository, candidate["id"])
        if not chain:
            return
        if before is None or not is_faded(before, after):
            return
        self._rebuild_cadence(
            connection, chain, candidate, actor, chain["data"]["transient_type"],
            "candidate_faded",
            {"previous_magnitude": before, "latest_magnitude": after},
        )

    def _reanchor(self, chain, candidate, actor):
        anchor = self._latest_anchor(candidate)
        if chain["data"].get("anchor_at") != anchor:
            chain["data"].setdefault("anchor_history", []).append(
                {"from": chain["data"].get("anchor_at"), "to": anchor, "by": actor.user_id}
            )
            chain["data"]["anchor_at"] = anchor

    def _rebuild_cadence(self, connection, chain, candidate, actor, new_type, cause, detail):
        """Recalculate the remaining chain, keeping completed segments."""
        self._reanchor(chain, candidate, actor)
        self._invalidate_pending(connection, chain, actor, cause, detail)
        chain["data"]["transient_type"] = new_type
        delays, duration_hours = cadence_for(new_type)
        chain["data"]["cadence_hours"] = delays
        chain["data"]["duration_hours"] = duration_hours
        completed = [item for item in chain["data"]["segments"] if item.get("state") == "completed"]
        chain["data"]["segments"] = completed + [
            {"state": "pending", "observation_id": None, "attempts": 0}
            for _ in range(len(completed), len(delays))
        ]
        self._persist(connection, self.repository, chain)
        self._audit(connection, chain["id"], actor, "chain_replanned",
                    chain["status"], chain["status"],
                    {"cause": cause, "detail": detail, "completed_segments": len(completed)})
        if len(completed) < len(delays):
            self._plan_next_tx(connection, chain, actor, cause)
        else:
            self._finish_if_complete(connection, chain, actor)

    def _invalidate_pending(self, connection, chain, actor, cause, detail):
        """Move every non-completed segment to history and free its booking.

        Completed segments are deliberately left untouched ("已完成记录留着").
        """
        history = chain["data"].setdefault("invalidated_history", [])
        touched = []
        for index, segment in enumerate(chain["data"].get("segments", [])):
            if segment.get("state") == "completed":
                continue
            record = dict(segment)
            record.update({"cadence_index": index, "cause": cause, "invalidated_at": utcnow()})
            observation_id = segment.get("observation_id")
            if observation_id:
                observation = self.repository._load_entity(connection, observation_id)
                if observation and observation["status"] not in ("completed", "withdrawn"):
                    record["observation_status"] = observation["status"]
                    self.repository._update_entity(
                        connection, observation_id, observation["version"], "withdrawn",
                        dict(observation["data"], invalidated_by_chain=chain["id"],
                             invalidation_cause=cause),
                    )
                    self._audit(connection, observation_id, actor, "chain_invalidate",
                                observation["status"], "withdrawn",
                                {"chain_id": chain["id"], "cause": cause, "detail": detail})
            history.append(record)
            touched.append(index)
        for index in touched:
            chain["data"]["segments"][index] = {
                "state": "invalidated",
                "observation_id": chain["data"]["segments"][index].get("observation_id"),
                "cause": cause,
            }
        self._persist(connection, self.repository, chain)
        self._audit(connection, chain["id"], actor, "segments_invalidated",
                    chain["status"], chain["status"],
                    {"cause": cause, "detail": detail, "invalidated": touched})
