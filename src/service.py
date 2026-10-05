from uuid import uuid4

from .audit import AuditTrail
from .chains import RelayChainManager
from .domain import ConflictError, NotFoundError, ValidationError
from .rules import RuleEngine

# Candidate actions that must move the candidate and its relay chain together.
_CANDIDATE_CHAIN_HOOKS = ("withdraw", "reclassify", "merge_measurement")


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)
        self.chains = RelayChainManager(repository, self.rules)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def _tx_lookup(self, connection):
        def lookup(kind, field, value):
            return self.repository._find_entities(
                connection, self.rules.normalize_kind(kind), field, value
            )
        return lookup

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        if kind == "chain":
            raise ValidationError("relay chains are started with start_chain on a candidate")
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
        return entity

    # -- transition routing ---------------------------------------------------

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        kind = self.rules.normalize_kind(entity["kind"])
        payload = dict(data or {})

        if kind == "candidate" and action == "start_chain":
            return self.chains.start(actor, entity_id, payload, expected_version)
        if kind == "chain" and action == "retry_segment":
            return self.chains.retry_segment(
                actor, entity_id, payload.get("segment_index"), expected_version
            )
        if kind == "observation" and action == "complete" and entity["data"].get("chain_id"):
            return self.chains.complete_segment(actor, entity_id, payload, expected_version)
        if kind == "observation" and action in ("withdraw", "correct") and entity["data"].get("chain_id"):
            return self._chain_observation_transition(
                actor, entity_id, action, payload, expected_version
            )
        if kind == "candidate" and action in _CANDIDATE_CHAIN_HOOKS:
            return self._candidate_transition(actor, entity_id, action, payload, expected_version)
        return self._plain_transition(actor, entity, action, payload, expected_version)

    def _plain_transition(self, actor, entity, action, data, expected_version):
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, data, self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity["id"], expected, next_status, merged)
        self.audit.record(
            entity["id"], actor, action, entity["status"], updated["status"], {"patch": patch}
        )
        return updated

    def _chain_observation_transition(self, actor, entity_id, action, data, expected_version):
        """Apply withdraw/correct to a chain-owned observation in one transaction."""
        with self.repository.transaction() as connection:
            entity = self.repository._load_entity(connection, entity_id)
            expected = int(expected_version) if expected_version is not None else entity["version"]
            next_status, patch = self.rules.validate_transition(
                actor, entity, action, data, self._tx_lookup(connection)
            )
            merged = dict(entity["data"])
            merged.update(patch)
            self.repository._update_entity(connection, entity_id, expected, next_status, merged)
            updated = self.repository._load_entity(connection, entity_id)
            self.repository._append_audit(
                connection, entity_id, actor.user_id, actor.role, action,
                entity["status"], next_status, {"patch": patch},
            )
            if action == "withdraw":
                self.chains.on_observation_withdraw(
                    connection, updated, actor, data.get("reason")
                )
            return self.repository._load_entity(connection, entity_id)

    def _candidate_transition(self, actor, entity_id, action, data, expected_version):
        """Validate and apply a candidate action and adjust its chain atomically."""
        with self.repository.transaction() as connection:
            entity = self.repository._load_entity(connection, entity_id)
            expected = int(expected_version) if expected_version is not None else entity["version"]
            next_status, patch = self.rules.validate_transition(
                actor, entity, action, data, self._tx_lookup(connection)
            )

            before_magnitude = None
            if action == "merge_measurement":
                prior = sorted(
                    entity["data"].get("measurements", []),
                    key=lambda item: item["observed_at"],
                )
                before_magnitude = prior[-1]["magnitude"] if prior else None
                after_magnitude = data["measurement"]["magnitude"]

            merged = dict(entity["data"])
            merged.update(patch)
            self.repository._update_entity(connection, entity_id, expected, next_status, merged)
            updated = self.repository._load_entity(connection, entity_id)
            self.repository._append_audit(
                connection, entity_id, actor.user_id, actor.role, action,
                entity["status"], next_status, {"patch": patch},
            )

            if action == "withdraw":
                self.chains.on_candidate_withdraw(
                    connection, updated, actor, data.get("reason")
                )
            elif action == "reclassify":
                self.chains.on_candidate_reclassify(
                    connection, updated, actor, data["new_type"], data.get("reason")
                )
            elif action == "merge_measurement":
                self.chains.on_candidate_measurement(
                    connection, updated, actor, before_magnitude, after_magnitude
                )
            return self.repository._load_entity(connection, entity_id)

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
