from uuid import uuid4

from .audit import AuditTrail
from .domain import Actor, ConflictError, InvalidTransition, NotFoundError, ValidationError
from .repository import utcnow
from .rules import (
    RELAY_HORIZON_DAYS,
    RuleEngine,
    find_free_window,
    next_segment_window,
)


SYSTEM_ACTOR = Actor("relay-chain", "admin")


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    # ------------------------------------------------------------------
    # Relay chain helpers
    # ------------------------------------------------------------------
    def _relay_mutator(self, candidate_id, mutate):
        """Apply ``mutate(relay)`` to the candidate's relay with optimistic retries."""
        for _ in range(8):
            entity = self.repository.get_entity(candidate_id)
            if not entity:
                raise NotFoundError("entity not found: " + candidate_id)
            relay = dict(entity["data"].get("relay") or {})
            mutate(relay)
            merged = dict(entity["data"])
            merged["relay"] = relay
            try:
                return self.repository.update_entity(
                    candidate_id, entity["version"], entity["status"], merged
                )
            except ConflictError:
                continue
        raise ConflictError("relay update conflict after retries")

    def _find_segment(self, candidate_id, segment_no):
        for obs in self.repository.list_entities(kind="observation"):
            data = obs["data"]
            if (
                obs["status"] != "withdrawn"
                and data.get("candidate_id") == candidate_id
                and int(data.get("segment_no", 0) or 0) == int(segment_no)
            ):
                return obs
        return None

    def _occupied_windows(self, telescope_id, team_id, exclude_candidate_id=None):
        occupied = []
        for obs in self.repository.list_entities(kind="observation", status="scheduled"):
            data = obs["data"]
            if data.get("candidate_id") == exclude_candidate_id:
                continue
            if data.get("telescope_id") == telescope_id or data.get("team_id") == team_id:
                occupied.append((data.get("start_at"), data.get("end_at")))
        return occupied

    def _record_failure(self, candidate_id, segment_no, reason):
        def mutate(relay):
            failure = dict(relay.get("failure") or {})
            attempts = int(failure.get("attempts", 0)) + 1
            relay["failure"] = {
                "segment_no": int(segment_no),
                "reason": str(reason),
                "attempts": attempts,
                "at": utcnow(),
            }

        self._relay_mutator(candidate_id, mutate)

    def _create_relay_segment(self, candidate, segment_no, start_at, end_at):
        relay = candidate["data"].get("relay") or {}
        payload = {
            "candidate_id": candidate["id"],
            "telescope_id": relay.get("telescope_id"),
            "team_id": relay.get("team_id"),
            "start_at": start_at,
            "end_at": end_at,
            "mode": "imaging",
            "segment_no": int(segment_no),
        }
        return self.create(SYSTEM_ACTOR, "observation", payload)

    def _advance_relay(self, actor, candidate_id, expected_version=None):
        """Create the next segment of the relay chain.

        Idempotent: if the segment already exists (e.g. a prior attempt wrote
        it but the response was lost), it is returned without occupying the
        telescope again.  Returns ``(segment, failure_reason)``.
        """
        candidate = self.repository.get_entity(candidate_id)
        if not candidate:
            raise NotFoundError("entity not found: " + candidate_id)
        self.rules.validate_relay_advance(actor, candidate)
        if expected_version is not None and int(candidate["version"]) != int(expected_version):
            raise ConflictError(
                "version conflict: expected %s, found %s"
                % (expected_version, candidate["version"])
            )
        relay = candidate["data"].get("relay") or {}
        if relay.get("status") == "invalidated":
            raise ValidationError("relay is invalidated: " + str(relay.get("reason")))
        segment_no = int(relay.get("last_completed", 0)) + 1

        existing = self._find_segment(candidate_id, segment_no)
        if existing:
            self._relay_mutator(candidate_id, lambda r: r.update({"failure": None}))
            return existing, None

        telescope_id = relay.get("telescope_id")
        team_id = relay.get("team_id")
        if not telescope_id or not team_id:
            reason = "relay is missing telescope or team"
            self._record_failure(candidate_id, segment_no, reason)
            return None, reason

        event_type = candidate["data"].get("transient_type")
        base = relay.get("base_observed_at")
        if not base:
            measurements = candidate["data"].get("measurements") or []
            base = measurements[-1]["observed_at"] if measurements else None
        if not base:
            reason = "cannot compute relay window: missing measurement time"
            self._record_failure(candidate_id, segment_no, reason)
            return None, reason

        ideal_start, ideal_end = next_segment_window(event_type, base, segment_no)
        occupied = self._occupied_windows(
            telescope_id, team_id, exclude_candidate_id=candidate_id
        )
        window = find_free_window(ideal_start, ideal_end, occupied)
        if not window:
            reason = "no free window within %s days for segment %s" % (
                RELAY_HORIZON_DAYS,
                segment_no,
            )
            self._record_failure(candidate_id, segment_no, reason)
            return None, reason
        start_at, end_at = window

        try:
            segment = self._create_relay_segment(candidate, segment_no, start_at, end_at)
        except Exception as exc:
            reason = "failed to write segment %s: %s" % (segment_no, exc)
            self._record_failure(candidate_id, segment_no, reason)
            return None, reason

        self._relay_mutator(candidate_id, lambda r: r.update({"failure": None}))
        return segment, None

    def _init_relay_from_observation(self, obs):
        candidate_id = obs["data"].get("candidate_id")
        candidate = self.repository.get_entity(candidate_id) if candidate_id else None
        if not candidate:
            return
        measurements = candidate["data"].get("measurements") or []
        base_observed_at = measurements[-1]["observed_at"] if measurements else obs["data"].get("start_at")
        base_magnitude = measurements[-1]["magnitude"] if measurements else None

        def mutate(relay):
            relay.setdefault("status", "active")
            relay.setdefault("last_completed", 0)
            relay["telescope_id"] = obs["data"].get("telescope_id")
            relay["team_id"] = obs["data"].get("team_id")
            relay.setdefault("base_observed_at", base_observed_at)
            relay.setdefault("base_magnitude", base_magnitude)
            relay.setdefault("failure", None)

        self._relay_mutator(candidate_id, mutate)

    def _system_withdraw(self, obs, reason):
        if obs["status"] not in ("requested", "scheduled"):
            return None
        updated = self.repository.update_entity(obs["id"], obs["version"], "withdrawn", obs["data"])
        self.audit.record(
            obs["id"],
            SYSTEM_ACTOR,
            "withdraw",
            obs["status"],
            "withdrawn",
            {"reason": reason, "system": True},
        )
        return updated

    def _pending_relay_observations(self, candidate_id):
        pending = []
        for obs in self.repository.list_entities(kind="observation"):
            data = obs["data"]
            if data.get("candidate_id") != candidate_id:
                continue
            if data.get("segment_no") is None:
                continue
            if obs["status"] in ("requested", "scheduled"):
                pending.append(obs)
        return pending

    def _invalidate_relay(self, actor, candidate, reason):
        for obs in self._pending_relay_observations(candidate["id"]):
            self._system_withdraw(obs, "relay invalidated: " + reason)

        def mutate(relay):
            relay["status"] = "invalidated"
            relay["reason"] = reason
            relay["failure"] = None

        self._relay_mutator(candidate["id"], mutate)

    def _recompute_relay(self, actor, candidate, reason):
        for obs in self._pending_relay_observations(candidate["id"]):
            self._system_withdraw(obs, "relay recompute: " + reason)

        measurements = candidate["data"].get("measurements") or []
        base_observed_at = measurements[-1]["observed_at"] if measurements else None
        base_magnitude = measurements[-1]["magnitude"] if measurements else None

        def mutate(relay):
            relay["status"] = "active"
            relay["reason"] = reason
            relay["base_observed_at"] = base_observed_at
            relay["base_magnitude"] = base_magnitude
            relay["failure"] = None

        self._relay_mutator(candidate["id"], mutate)
        self._advance_relay(SYSTEM_ACTOR, candidate["id"])

    def _maybe_recompute_for_dimming(self, actor, candidate):
        relay = candidate["data"].get("relay") or {}
        if relay.get("status") == "invalidated":
            return
        base_magnitude = relay.get("base_magnitude")
        latest_magnitude = candidate["data"].get("latest_magnitude")
        if base_magnitude is None or latest_magnitude is None:
            return
        try:
            dimmed = float(latest_magnitude) - float(base_magnitude)
        except (TypeError, ValueError):
            return
        if dimmed > 2:
            self._recompute_relay(actor, candidate, "dimmed by %.2f magnitudes" % dimmed)

    def _after_complete(self, actor, obs):
        segment_no = obs["data"].get("segment_no")
        if segment_no is None:
            return
        candidate_id = obs["data"].get("candidate_id")
        try:
            self._relay_mutator(
                candidate_id,
                lambda r: r.update(
                    {
                        "last_completed": max(
                            int(r.get("last_completed", 0)), int(segment_no)
                        ),
                        "failure": None,
                    }
                ),
            )
            self._advance_relay(SYSTEM_ACTOR, candidate_id)
        except Exception as exc:
            try:
                self._record_failure(
                    candidate_id, int(segment_no) + 1, "relay advance failed: %s" % exc
                )
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Use cases
    # ------------------------------------------------------------------
    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        validated = self.rules.validate_create(actor, kind, payload, self._lookup)
        if validated:
            payload.update(validated)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        if kind == "observation" and payload.get("segment_no") is not None:
            self._init_relay_from_observation(entity)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]

        if entity["kind"] == "candidate" and action in self.rules.RELAY_ACTIONS:
            if action == "advance_relay":
                segment, failure = self._advance_relay(actor, entity["id"], expected)
                if failure:
                    raise ConflictError(failure)
                return segment
            raise InvalidTransition("unknown relay action: " + action)

        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )

        if entity["kind"] == "observation" and action == "complete":
            self._after_complete(actor, updated)
        elif entity["kind"] == "candidate":
            if action == "withdraw":
                self._invalidate_relay(actor, updated, "withdrawn")
            elif action == "reclassify":
                self._recompute_relay(actor, updated, "reclassified")
            elif action == "merge_measurement":
                self._maybe_recompute_for_dimming(actor, updated)
        return updated

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
