from datetime import datetime, timedelta

from .domain import ConflictError, InvalidTransition, PermissionDenied, ValidationError


def _find_one(lookup, kind, field, value):
    if lookup is None:
        return None
    rows = lookup(kind, field, value) or []
    return rows[0] if rows else None


RELAY_HORIZON_DAYS = 14

# Cadence of a relay chain: segment number (1-based) -> (offset from the
# measurement instant, exposure duration).  Offsets grow so each segment is
# scheduled later than the previous one.
RELAY_CADENCE = {
    "grb": [
        (timedelta(minutes=5), timedelta(minutes=10)),
        (timedelta(hours=1), timedelta(minutes=15)),
        (timedelta(hours=6), timedelta(minutes=20)),
        (timedelta(days=1), timedelta(minutes=30)),
        (timedelta(days=3), timedelta(minutes=30)),
    ],
    "supernova": [
        (timedelta(hours=12), timedelta(minutes=30)),
        (timedelta(days=2), timedelta(minutes=45)),
        (timedelta(days=7), timedelta(hours=1)),
        (timedelta(days=21), timedelta(hours=1)),
        (timedelta(days=60), timedelta(hours=2)),
    ],
    "tde": [
        (timedelta(days=1), timedelta(minutes=30)),
        (timedelta(days=7), timedelta(hours=1)),
        (timedelta(days=30), timedelta(hours=2)),
        (timedelta(days=90), timedelta(hours=2)),
    ],
    "variable": [
        (timedelta(days=1), timedelta(minutes=20)),
        (timedelta(days=7), timedelta(minutes=20)),
        (timedelta(days=30), timedelta(minutes=30)),
    ],
    "unknown": [
        (timedelta(hours=1), timedelta(minutes=15)),
        (timedelta(days=1), timedelta(minutes=30)),
        (timedelta(days=7), timedelta(hours=1)),
    ],
}

DEFAULT_RELAY_CADENCE = RELAY_CADENCE["unknown"]


def _parse_iso(value):
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _format_iso(value):
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def relay_cadence(event_type, segment_no):
    table = RELAY_CADENCE.get(event_type, DEFAULT_RELAY_CADENCE)
    index = min(max(int(segment_no) - 1, 0), len(table) - 1)
    return table[index]


def next_segment_window(event_type, observed_at, segment_no):
    """Compute the ideal window for a segment from the measurement instant."""
    offset, duration = relay_cadence(event_type, int(segment_no))
    start = _parse_iso(observed_at) + offset
    return _format_iso(start), _format_iso(start + duration)


def find_free_window(ideal_start, ideal_end, occupied):
    """Slide the ideal window forward to the earliest slot free of overlap.

    ``occupied`` is an iterable of (start, end) string intervals belonging to
    either the telescope or the team.  Returns None when no slot opens within
    ``RELAY_HORIZON_DAYS``.
    """
    start = _parse_iso(ideal_start)
    duration = _parse_iso(ideal_end) - start
    if duration.total_seconds() <= 0:
        return None
    limit = start + timedelta(days=RELAY_HORIZON_DAYS)
    occupied_dt = []
    for item in occupied:
        try:
            occupied_dt.append((_parse_iso(item[0]), _parse_iso(item[1])))
        except (ValueError, TypeError, IndexError):
            continue
    candidates = {start}
    for _, occupied_end in occupied_dt:
        if start <= occupied_end <= limit:
            candidates.add(occupied_end)
    for candidate in sorted(candidates):
        if candidate > limit:
            continue
        candidate_end = candidate + duration
        if candidate_end > limit:
            continue
        if any(candidate < occupied_end and occupied_start < candidate_end
               for occupied_start, occupied_end in occupied_dt):
            continue
        return _format_iso(candidate), _format_iso(candidate_end)
    return None


def calculate_priority(magnitude, transient_type):
    """Return a deterministic observing priority from 0 to 100."""
    try:
        mag = float(magnitude)
    except (TypeError, ValueError):
        raise ValidationError("magnitude must be numeric")
    if mag < -30 or mag > 40:
        raise ValidationError("magnitude is outside the supported range")
    type_weights = {
        "grb": 30,
        "supernova": 22,
        "tde": 20,
        "variable": 8,
        "unknown": 12,
    }
    score = max(0.0, 100.0 - mag * 4.0) + type_weights.get(transient_type, 5)
    return round(min(100.0, score), 2)


def measurements_overlap(first_start, first_end, second_start, second_end):
    return str(first_start) < str(second_end) and str(second_start) < str(first_end)


def _validate_source(actor, data, lookup):
    if len(str(data.get("name", "")).strip()) < 2:
        raise ValidationError("source name is required")
    return {}


def _validate_candidate(actor, data, lookup):
    source_id = data.get("source_id")
    if not _find_one(lookup, "source", "id", source_id):
        raise ValidationError("source does not exist")
    try:
        ra = float(data.get("ra"))
        dec = float(data.get("dec"))
    except (TypeError, ValueError):
        raise ValidationError("ra and dec must be numeric")
    if ra < 0 or ra >= 360 or dec < -90 or dec > 90:
        raise ValidationError("coordinates are outside valid ranges")
    event_key = "%s:%s" % (source_id, data.get("event_id"))
    duplicate = _find_one(lookup, "candidate", "event_key", event_key)
    if duplicate:
        raise ConflictError("candidate already exists for event key " + event_key)
    return {
        "event_key": event_key,
        "measurements": [
            {
                "observed_at": data.get("observed_at"),
                "ra": ra,
                "dec": dec,
                "magnitude": float(data["magnitude"]),
            }
        ],
        "priority_score": calculate_priority(data.get("magnitude"), data.get("transient_type")),
    }


def _validate_telescope(actor, data, lookup):
    if not (0 < float(data.get("aperture_m", 0))):
        raise ValidationError("aperture_m must be positive")
    return {}


def _validate_observation(actor, data, lookup):
    candidate = _find_one(lookup, "candidate", "id", data.get("candidate_id"))
    if not candidate:
        raise ValidationError("candidate does not exist")
    if not _find_one(lookup, "telescope", "id", data.get("telescope_id")):
        raise ValidationError("telescope does not exist")
    if str(data.get("start_at")) >= str(data.get("end_at")):
        raise ValidationError("observation end must be after start")
    extra = {"scheduled_team": data.get("team_id")}
    segment_no = data.get("segment_no")
    if segment_no is not None:
        try:
            segment_no = int(segment_no)
        except (TypeError, ValueError):
            raise ValidationError("segment_no must be an integer")
        if segment_no < 1:
            raise ValidationError("segment_no must be positive")
        relay = candidate["data"].get("relay") or {}
        last_completed = int(relay.get("last_completed", 0))
        if segment_no > last_completed + 1:
            raise ValidationError(
                "next relay segment is %s, got %s" % (last_completed + 1, segment_no)
            )
        for obs in lookup("observation", "candidate_id", candidate["id"]) or []:
            if obs["status"] == "withdrawn":
                continue
            if int(obs["data"].get("segment_no", 0) or 0) == segment_no:
                raise ConflictError("relay segment %s already exists" % segment_no)
        extra["segment_no"] = segment_no
    return extra


def _validate_merge_measurement(actor, entity, data, lookup):
    measurement = data.get("measurement")
    if not isinstance(measurement, dict):
        raise ValidationError("measurement must be an object")
    try:
        ra = float(measurement.get("ra"))
        dec = float(measurement.get("dec"))
        magnitude = float(measurement.get("magnitude"))
    except (TypeError, ValueError):
        raise ValidationError("measurement coordinates and magnitude must be numeric")
    if ra < 0 or ra >= 360 or dec < -90 or dec > 90:
        raise ValidationError("measurement coordinates are outside valid ranges")
    observed_at = str(measurement.get("observed_at", "")).strip()
    if not observed_at:
        raise ValidationError("measurement observed_at is required")
    existing = list(entity["data"].get("measurements") or [])
    if any(item.get("observed_at") == observed_at for item in existing):
        raise ConflictError("measurement timestamp already merged")
    merged = existing + [{"observed_at": observed_at, "ra": ra, "dec": dec, "magnitude": magnitude}]
    latest = sorted(merged, key=lambda item: item["observed_at"])[-1]
    return {
        "measurements": merged,
        "merged_measurement_count": len(merged),
        "latest_magnitude": latest["magnitude"],
        "priority_score": calculate_priority(latest["magnitude"], entity["data"].get("transient_type")),
    }


def _validate_reclassify(actor, entity, data, lookup):
    if data.get("new_type") not in {"grb", "supernova", "tde", "variable", "unknown"}:
        raise ValidationError("unsupported transient type")
    return {"transient_type": data["new_type"], "previous_type": entity["data"].get("transient_type")}


def _validate_correct(actor, entity, data, lookup):
    if not str(data.get("reason", "")).strip():
        raise ValidationError("correction reason is required")
    return {"corrected_by": actor.user_id}


def _validate_schedule(actor, entity, data, lookup):
    observations = lookup("observation", "telescope_id", entity["data"].get("telescope_id")) if lookup else []
    for other in observations:
        if other["id"] == entity["id"] or other["status"] != "scheduled":
            continue
        if measurements_overlap(
            entity["data"].get("start_at"),
            entity["data"].get("end_at"),
            other["data"].get("start_at"),
            other["data"].get("end_at"),
        ):
            raise ConflictError("telescope is already scheduled in this window")
    team_observations = lookup("observation", "team_id", entity["data"].get("team_id")) if lookup else []
    for other in team_observations:
        if other["id"] == entity["id"] or other["status"] != "scheduled":
            continue
        if measurements_overlap(
            entity["data"].get("start_at"),
            entity["data"].get("end_at"),
            other["data"].get("start_at"),
            other["data"].get("end_at"),
        ):
            raise ConflictError("observation team is already committed in this window")
    return {"scheduled_by": actor.user_id}


class RuleEngine:
    ALIASES = {
        "sources": "source",
        "candidates": "candidate",
        "telescopes": "telescope",
        "observations": "observation",
    }
    INITIAL_STATUS = {
        "source": "registered",
        "candidate": "detected",
        "telescope": "available",
        "observation": "requested",
    }
    TRANSITIONS = {
        "source": {
            "activate": (("registered",), "active"),
            "retire": (("active",), "retired"),
        },
        "candidate": {
            "merge_measurement": (("detected", "triaged"), "triaged"),
            "triage": (("detected",), "triaged"),
            "reclassify": (("triaged",), "triaged"),
            "correct": (("detected", "triaged", "classified"), "triaged"),
            "withdraw": (("detected", "triaged"), "withdrawn"),
            "classify": (("triaged",), "classified"),
        },
        "telescope": {
            "restrict": (("available",), "restricted"),
            "restore": (("restricted",), "available"),
        },
        "observation": {
            "schedule": (("requested",), "scheduled"),
            "complete": (("scheduled",), "completed"),
            "withdraw": (("requested", "scheduled"), "withdrawn"),
            "correct": (("requested", "scheduled"), "requested"),
        },
    }
    CREATE_REQUIRED = {
        "source": ("name", "survey_name"),
        "candidate": ("source_id", "event_id", "ra", "dec", "magnitude", "transient_type", "observed_at"),
        "telescope": ("name", "aperture_m", "site_name"),
        "observation": ("candidate_id", "telescope_id", "team_id", "start_at", "end_at", "mode"),
    }
    ACTION_REQUIRED = {
        ("source", "retire"): ("reason",),
        ("candidate", "merge_measurement"): ("measurement",),
        ("candidate", "triage"): ("reason",),
        ("candidate", "reclassify"): ("new_type", "reason"),
        ("candidate", "correct"): ("reason",),
        ("candidate", "withdraw"): ("reason",),
        ("candidate", "classify"): ("classification",),
        ("telescope", "restrict"): ("reason",),
        ("observation", "schedule"): ("operator_id",),
        ("observation", "withdraw"): ("reason",),
        ("observation", "correct"): ("reason",),
    }
    CREATE_ROLES = {
        "source": ("analyst", "admin"),
        "candidate": ("analyst", "operator", "admin"),
        "telescope": ("coordinator", "admin"),
        "observation": ("analyst", "coordinator", "admin"),
    }
    RELAY_ACTIONS = frozenset({"advance_relay"})
    RELAY_ROLES = ("analyst", "coordinator", "admin")
    ROLE_ACTIONS = {
        "activate": ("coordinator", "admin"),
        "retire": ("coordinator", "admin"),
        "merge_measurement": ("analyst", "operator", "admin"),
        "triage": ("analyst", "operator", "admin"),
        "reclassify": ("analyst", "supervisor", "admin"),
        "correct": ("analyst", "supervisor", "admin"),
        "withdraw": ("analyst", "supervisor", "admin"),
        "classify": ("analyst", "supervisor", "admin"),
        "restrict": ("coordinator", "admin"),
        "restore": ("coordinator", "admin"),
        "schedule": ("coordinator", "admin"),
        "complete": ("operator", "coordinator", "admin"),
    }
    CUSTOM_CREATE = {
        "source": _validate_source,
        "candidate": _validate_candidate,
        "telescope": _validate_telescope,
        "observation": _validate_observation,
    }
    CUSTOM_TRANSITIONS = {
        ("candidate", "merge_measurement"): _validate_merge_measurement,
        ("candidate", "reclassify"): _validate_reclassify,
        ("candidate", "correct"): _validate_correct,
        ("observation", "schedule"): _validate_schedule,
        ("observation", "correct"): _validate_correct,
    }

    def normalize_kind(self, kind):
        return self.ALIASES.get(kind, kind)

    def initial_status(self, kind):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        return self.INITIAL_STATUS[kind]

    @staticmethod
    def _ensure_role(actor, allowed):
        if actor.role not in allowed:
            raise PermissionDenied("role %s is not allowed here" % actor.role)

    @staticmethod
    def _require(data, fields):
        for field in fields:
            value = data.get(field)
            if value is None or value == "" or value == [] or value == {}:
                raise ValidationError("missing required field: " + field)

    def validate_create(self, actor, kind, data, lookup=None):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        self._ensure_role(actor, self.CREATE_ROLES.get(kind, ("admin",)))
        self._require(data, self.CREATE_REQUIRED.get(kind, ()))
        custom = self.CUSTOM_CREATE.get(kind)
        return custom(actor, data, lookup) if custom else {}

    def validate_transition(self, actor, entity, action, data, lookup=None):
        kind = self.normalize_kind(entity["kind"])
        transition = self.TRANSITIONS.get(kind, {}).get(action)
        if not transition:
            raise InvalidTransition("unknown action %s for %s" % (action, kind))
        allowed_statuses, next_status = transition
        if entity["status"] not in allowed_statuses:
            raise InvalidTransition("cannot %s from status %s" % (action, entity["status"]))
        allowed_roles = self.ROLE_ACTIONS.get((kind, action), self.ROLE_ACTIONS.get(action, ("admin",)))
        self._ensure_role(actor, allowed_roles)
        self._require(data, self.ACTION_REQUIRED.get((kind, action), ()))
        custom = self.CUSTOM_TRANSITIONS.get((kind, action))
        extra = custom(actor, entity, data, lookup) if custom else {}
        patch = dict(data)
        if extra:
            patch.update(extra)
        return next_status, patch

    def validate_relay_advance(self, actor, entity):
        self._ensure_role(actor, self.RELAY_ROLES)
        if entity["kind"] != "candidate":
            raise ValidationError("relay advance is only valid for candidates")
        if entity["status"] == "withdrawn":
            raise InvalidTransition("cannot advance relay for a withdrawn candidate")
