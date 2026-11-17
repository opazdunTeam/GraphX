-- +goose Up

LOCK TABLE entity_names, entity_identifiers IN SHARE ROW EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM entity_names)
        OR EXISTS (SELECT 1 FROM entity_identifiers) THEN
        RAISE EXCEPTION
            'migration 006 requires empty entity projection tables before entity links exist';
    END IF;
END;
$$;
-- +goose StatementEnd

CREATE TABLE match_reviews (
    id UUID PRIMARY KEY,
    source_record_id UUID NOT NULL REFERENCES source_records(id) ON DELETE RESTRICT,
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'decided')),
    candidate_set_version INTEGER NOT NULL DEFAULT 1 CHECK (candidate_set_version >= 1),
    lock_version BIGINT NOT NULL DEFAULT 1 CHECK (lock_version >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    CHECK ((status = 'open' AND resolved_at IS NULL) OR (status = 'decided' AND resolved_at IS NOT NULL)),
    CHECK (resolved_at IS NULL OR resolved_at >= created_at)
);

CREATE INDEX idx_match_reviews_source_record ON match_reviews (source_record_id);
CREATE UNIQUE INDEX idx_match_reviews_open_record ON match_reviews (source_record_id) WHERE status = 'open';

-- +goose StatementBegin
CREATE FUNCTION assert_record_canonicalization_allowed(record_id UUID)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    investigation_status TEXT;
BEGIN
    SELECT investigation.status INTO investigation_status
    FROM source_records AS record
    JOIN processing_runs AS run ON run.id = record.processing_run_id
    JOIN source_job_results AS result ON result.id = run.source_job_result_id
    JOIN source_jobs AS job ON job.id = result.source_job_id
    JOIN investigations AS investigation ON investigation.id = job.investigation_id
    WHERE record.id = record_id
    FOR UPDATE OF investigation;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'source record % has no investigation provenance', record_id;
    END IF;
    IF investigation_status <> 'running' THEN
        RAISE EXCEPTION 'canonicalization is not allowed while investigation status is %',
            investigation_status;
    END IF;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION validate_match_review_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status <> 'open'
        OR NEW.resolved_at IS NOT NULL
        OR NEW.candidate_set_version <> 1
        OR NEW.lock_version <> 1 THEN
        RAISE EXCEPTION 'a match review must start open at candidate/lock version 1/1';
    END IF;
    PERFORM assert_record_canonicalization_allowed(NEW.source_record_id);
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER match_reviews_validate_insert
BEFORE INSERT ON match_reviews FOR EACH ROW EXECUTE FUNCTION validate_match_review_insert();

-- +goose StatementBegin
CREATE FUNCTION validate_match_review_mutation() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'match review history cannot be deleted';
    END IF;
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.source_record_id IS DISTINCT FROM OLD.source_record_id
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'match review identity is immutable';
    END IF;
    IF pg_trigger_depth() > 1 THEN
        RETURN NEW;
    END IF;
    IF NEW.status IS DISTINCT FROM OLD.status OR NEW.resolved_at IS DISTINCT FROM OLD.resolved_at THEN
        RAISE EXCEPTION 'match review status is controlled by match decisions';
    END IF;
    IF NEW.candidate_set_version IS DISTINCT FROM OLD.candidate_set_version THEN
        IF OLD.status <> 'open'
            OR NEW.candidate_set_version <> OLD.candidate_set_version + 1
            OR NEW.lock_version <> OLD.lock_version + 1 THEN
            RAISE EXCEPTION 'candidate set refresh requires an open review and one version increment';
        END IF;
    ELSIF NEW.lock_version IS DISTINCT FROM OLD.lock_version THEN
        RAISE EXCEPTION 'lock_version may change only with a candidate refresh or decision';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER match_reviews_validate_mutation
BEFORE UPDATE OR DELETE ON match_reviews FOR EACH ROW EXECUTE FUNCTION validate_match_review_mutation();

CREATE TABLE match_candidates (
    id UUID PRIMARY KEY,
    match_review_id UUID NOT NULL REFERENCES match_reviews(id) ON DELETE RESTRICT,
    candidate_set_version INTEGER NOT NULL CHECK (candidate_set_version >= 1),
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE RESTRICT,
    score NUMERIC NOT NULL CHECK (score >= 0 AND score <= 1),
    features JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(features) = 'array'),
    conflicts JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(conflicts) = 'array'),
    recommendation TEXT NOT NULL CHECK (recommendation IN ('auto_link', 'review', 'reject')),
    algorithm_version TEXT NOT NULL CHECK (length(btrim(algorithm_version)) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (match_review_id, candidate_set_version, entity_id),
    UNIQUE (id, match_review_id, candidate_set_version)
);

CREATE INDEX idx_match_candidates_entity ON match_candidates (entity_id);

-- +goose StatementBegin
CREATE FUNCTION validate_match_candidate_mutation() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    review_status TEXT;
    current_candidate_set_version INTEGER;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'match candidates are append-only';
    END IF;
    SELECT status, candidate_set_version INTO review_status, current_candidate_set_version
    FROM match_reviews WHERE id = NEW.match_review_id FOR UPDATE;
    IF review_status <> 'open' THEN
        RAISE EXCEPTION 'candidates may be added only to an open review';
    END IF;
    IF NEW.candidate_set_version <> current_candidate_set_version THEN
        RAISE EXCEPTION 'candidate belongs to stale candidate set version';
    END IF;
    PERFORM assert_record_canonicalization_allowed(
        (SELECT source_record_id FROM match_reviews WHERE id = NEW.match_review_id)
    );
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER match_candidates_validate_mutation
BEFORE INSERT OR UPDATE OR DELETE ON match_candidates FOR EACH ROW EXECUTE FUNCTION validate_match_candidate_mutation();

CREATE TABLE match_decisions (
    id UUID PRIMARY KEY,
    match_review_id UUID NOT NULL REFERENCES match_reviews(id) ON DELETE RESTRICT,
    candidate_set_version INTEGER NOT NULL CHECK (candidate_set_version >= 1),
    selected_candidate_id UUID,
    resolved_entity_id UUID REFERENCES entities(id) ON DELETE RESTRICT,
    supersedes_decision_id UUID,
    expected_lock_version BIGINT NOT NULL CHECK (expected_lock_version >= 1),
    decision TEXT NOT NULL CHECK (decision IN ('select_candidate', 'none_of_above', 'revert')),
    actor_type TEXT NOT NULL CHECK (length(btrim(actor_type)) > 0),
    actor_id TEXT CHECK (actor_id IS NULL OR length(btrim(actor_id)) > 0),
    reason TEXT,
    algorithm_version TEXT CHECK (algorithm_version IS NULL OR length(btrim(algorithm_version)) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (id, match_review_id),
    UNIQUE (supersedes_decision_id),
    FOREIGN KEY (selected_candidate_id, match_review_id, candidate_set_version)
        REFERENCES match_candidates(id, match_review_id, candidate_set_version) ON DELETE RESTRICT,
    FOREIGN KEY (supersedes_decision_id, match_review_id)
        REFERENCES match_decisions(id, match_review_id) ON DELETE RESTRICT,
    CHECK (
        (decision = 'select_candidate' AND selected_candidate_id IS NOT NULL AND resolved_entity_id IS NOT NULL)
        OR (decision = 'none_of_above' AND selected_candidate_id IS NULL AND resolved_entity_id IS NOT NULL)
        OR (decision = 'revert' AND selected_candidate_id IS NULL AND resolved_entity_id IS NULL)
    ),
    CHECK (reason IS NULL OR length(btrim(reason)) BETWEEN 1 AND 2000),
    CHECK (decision <> 'revert' OR reason IS NOT NULL),
    CHECK (supersedes_decision_id IS NULL OR supersedes_decision_id <> id)
);

CREATE UNIQUE INDEX idx_match_decisions_one_root
    ON match_decisions (match_review_id) WHERE supersedes_decision_id IS NULL;
CREATE INDEX idx_match_decisions_review ON match_decisions (match_review_id);
CREATE INDEX idx_match_decisions_candidate ON match_decisions (selected_candidate_id)
    WHERE selected_candidate_id IS NOT NULL;
CREATE INDEX idx_match_decisions_resolved_entity ON match_decisions (resolved_entity_id)
    WHERE resolved_entity_id IS NOT NULL;

-- +goose StatementBegin
CREATE FUNCTION validate_match_decision_insert() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    review_status TEXT;
    current_candidate_set_version INTEGER;
    current_lock_version BIGINT;
    head_decision_id UUID;
    candidate_entity_id UUID;
BEGIN
    SELECT status, candidate_set_version, lock_version
    INTO review_status, current_candidate_set_version, current_lock_version
    FROM match_reviews WHERE id = NEW.match_review_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'match review % does not exist', NEW.match_review_id;
    END IF;
    IF NEW.expected_lock_version <> current_lock_version THEN
        RAISE EXCEPTION 'stale match review lock version: expected %, current %',
            NEW.expected_lock_version, current_lock_version;
    END IF;
    IF NEW.candidate_set_version <> current_candidate_set_version THEN
        RAISE EXCEPTION 'decision belongs to stale candidate set version';
    END IF;
    IF NEW.decision <> 'revert' THEN
        PERFORM assert_record_canonicalization_allowed(
            (SELECT source_record_id FROM match_reviews WHERE id = NEW.match_review_id)
        );
    END IF;
    SELECT decision.id INTO head_decision_id
    FROM match_decisions AS decision
    WHERE decision.match_review_id = NEW.match_review_id
      AND NOT EXISTS (SELECT 1 FROM match_decisions AS child WHERE child.supersedes_decision_id = decision.id);
    IF head_decision_id IS NULL THEN
        IF NEW.supersedes_decision_id IS NOT NULL OR NEW.decision = 'revert' OR review_status <> 'open' THEN
            RAISE EXCEPTION 'first decision must resolve an open review and have no predecessor';
        END IF;
    ELSE
        IF NEW.supersedes_decision_id IS DISTINCT FROM head_decision_id THEN
            RAISE EXCEPTION 'decision must supersede the current decision head';
        END IF;
        IF NEW.decision = 'revert' AND review_status <> 'decided' THEN
            RAISE EXCEPTION 'only a decided review can be reverted';
        END IF;
        IF NEW.decision <> 'revert' AND review_status <> 'open' THEN
            RAISE EXCEPTION 'a replacement decision requires the previous decision to be reverted';
        END IF;
    END IF;
    IF NEW.decision = 'select_candidate' THEN
        SELECT entity_id INTO candidate_entity_id
        FROM match_candidates
        WHERE id = NEW.selected_candidate_id
          AND match_review_id = NEW.match_review_id
          AND candidate_set_version = NEW.candidate_set_version;
        IF candidate_entity_id IS DISTINCT FROM NEW.resolved_entity_id THEN
            RAISE EXCEPTION 'resolved entity must match the selected candidate';
        END IF;
    ELSIF NEW.decision = 'none_of_above' AND EXISTS (
        SELECT 1 FROM match_candidates
        WHERE match_review_id = NEW.match_review_id
          AND candidate_set_version = NEW.candidate_set_version
          AND entity_id = NEW.resolved_entity_id
    ) THEN
        RAISE EXCEPTION 'none_of_above cannot resolve to an entity from the candidate set';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER match_decisions_validate_insert
BEFORE INSERT ON match_decisions FOR EACH ROW EXECUTE FUNCTION validate_match_decision_insert();

-- +goose StatementBegin
CREATE FUNCTION apply_match_decision_to_review() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.decision = 'revert' THEN
        UPDATE match_reviews SET status = 'open', resolved_at = NULL, lock_version = lock_version + 1
        WHERE id = NEW.match_review_id;
        UPDATE entity_records AS link
        SET active = FALSE, unlinked_at = NEW.created_at
        FROM match_reviews AS review
        WHERE review.id = NEW.match_review_id
          AND link.source_record_id = review.source_record_id
          AND link.active;
    ELSE
        UPDATE match_reviews SET status = 'decided', resolved_at = NEW.created_at, lock_version = lock_version + 1
        WHERE id = NEW.match_review_id;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER match_decisions_apply_to_review
AFTER INSERT ON match_decisions FOR EACH ROW EXECUTE FUNCTION apply_match_decision_to_review();

-- +goose StatementBegin
CREATE FUNCTION prevent_match_decision_mutation() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'match_decisions is append-only';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER match_decisions_append_only
BEFORE UPDATE OR DELETE ON match_decisions FOR EACH ROW EXECUTE FUNCTION prevent_match_decision_mutation();

CREATE TABLE entity_records (
    id UUID PRIMARY KEY,
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE RESTRICT,
    source_record_id UUID NOT NULL REFERENCES source_records(id) ON DELETE RESTRICT,
    match_decision_id UUID NOT NULL REFERENCES match_decisions(id) ON DELETE RESTRICT,
    link_method TEXT NOT NULL CHECK (length(btrim(link_method)) > 0),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    unlinked_at TIMESTAMPTZ,
    CHECK ((active AND unlinked_at IS NULL) OR (NOT active AND unlinked_at IS NOT NULL)),
    CHECK (unlinked_at IS NULL OR unlinked_at >= linked_at)
);

CREATE INDEX idx_entity_records_entity ON entity_records (entity_id);
CREATE INDEX idx_entity_records_source_record ON entity_records (source_record_id);
CREATE UNIQUE INDEX idx_entity_records_match_decision ON entity_records (match_decision_id);
CREATE UNIQUE INDEX idx_entity_records_active_source_record ON entity_records (source_record_id) WHERE active;

-- +goose StatementBegin
CREATE FUNCTION validate_entity_record_history() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    decision_record_id UUID;
    decision_entity_id UUID;
    decision_kind TEXT;
    review_status TEXT;
    has_successor BOOLEAN;
    review_id UUID;
    unlink_is_current_revert BOOLEAN;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'entity record history cannot be deleted';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF NOT OLD.active OR NEW.active
            OR NEW.id IS DISTINCT FROM OLD.id
            OR NEW.entity_id IS DISTINCT FROM OLD.entity_id
            OR NEW.source_record_id IS DISTINCT FROM OLD.source_record_id
            OR NEW.match_decision_id IS DISTINCT FROM OLD.match_decision_id
            OR NEW.link_method IS DISTINCT FROM OLD.link_method
            OR NEW.linked_at IS DISTINCT FROM OLD.linked_at
            OR NEW.unlinked_at IS NULL THEN
            RAISE EXCEPTION 'entity link may only transition once from active to inactive';
        END IF;
        SELECT EXISTS (
            SELECT 1
            FROM match_decisions AS original
            JOIN match_reviews AS review ON review.id = original.match_review_id
            JOIN match_decisions AS revert
              ON revert.match_review_id = review.id
             AND revert.supersedes_decision_id = original.id
             AND revert.decision = 'revert'
            WHERE original.id = OLD.match_decision_id
              AND review.source_record_id = OLD.source_record_id
              AND review.status = 'open'
              AND NEW.unlinked_at = revert.created_at
              AND NOT EXISTS (
                  SELECT 1 FROM match_decisions AS child
                  WHERE child.supersedes_decision_id = revert.id
              )
        ) INTO unlink_is_current_revert;
        IF NOT unlink_is_current_revert THEN
            RAISE EXCEPTION 'entity link may be deactivated only by its current revert decision';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NOT NEW.active OR NEW.unlinked_at IS NOT NULL THEN
            RAISE EXCEPTION 'an entity link must be inserted active and without unlinked_at';
        END IF;
        SELECT decision.match_review_id INTO review_id
        FROM match_decisions AS decision
        WHERE decision.id = NEW.match_decision_id;
        IF review_id IS NULL THEN
            RAISE EXCEPTION 'entity link decision % does not exist', NEW.match_decision_id;
        END IF;

        PERFORM 1 FROM match_reviews WHERE id = review_id FOR UPDATE;

        SELECT review.source_record_id, decision.resolved_entity_id, decision.decision, review.status,
               EXISTS (SELECT 1 FROM match_decisions AS child WHERE child.supersedes_decision_id = decision.id)
        INTO decision_record_id, decision_entity_id, decision_kind, review_status, has_successor
        FROM match_decisions AS decision
        JOIN match_reviews AS review ON review.id = decision.match_review_id
        WHERE decision.id = NEW.match_decision_id;
        IF decision_kind = 'revert' OR review_status <> 'decided' OR has_successor
            OR decision_record_id IS DISTINCT FROM NEW.source_record_id
            OR decision_entity_id IS DISTINCT FROM NEW.entity_id THEN
            RAISE EXCEPTION 'entity link does not match the current resolution decision';
        END IF;
        PERFORM assert_record_canonicalization_allowed(NEW.source_record_id);
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER entity_records_validate_history
BEFORE INSERT OR UPDATE OR DELETE ON entity_records FOR EACH ROW EXECUTE FUNCTION validate_entity_record_history();

-- +goose StatementBegin
CREATE FUNCTION validate_active_entity_projection()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.active THEN
        PERFORM 1
        FROM claims AS claim
        JOIN entity_records AS link
          ON link.source_record_id = claim.subject_source_record_id
         AND link.entity_id = NEW.entity_id
         AND link.active
        WHERE claim.id = NEW.claim_id
        FOR UPDATE OF link;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'active entity projection requires a matching active entity link';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER entity_names_validate_active_link
BEFORE INSERT OR UPDATE ON entity_names
FOR EACH ROW EXECUTE FUNCTION validate_active_entity_projection();

CREATE TRIGGER entity_identifiers_validate_active_link
BEFORE INSERT OR UPDATE ON entity_identifiers
FOR EACH ROW EXECUTE FUNCTION validate_active_entity_projection();

-- +goose StatementBegin
CREATE FUNCTION deactivate_entity_projections_for_link()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.active AND NOT NEW.active THEN
        UPDATE entity_names AS projection
        SET active = FALSE
        FROM claims AS claim
        WHERE projection.claim_id = claim.id
          AND claim.subject_source_record_id = OLD.source_record_id
          AND projection.entity_id = OLD.entity_id
          AND projection.active;

        UPDATE entity_identifiers AS projection
        SET active = FALSE
        FROM claims AS claim
        WHERE projection.claim_id = claim.id
          AND claim.subject_source_record_id = OLD.source_record_id
          AND projection.entity_id = OLD.entity_id
          AND projection.active;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER entity_records_deactivate_projections
AFTER UPDATE ON entity_records
FOR EACH ROW EXECUTE FUNCTION deactivate_entity_projections_for_link();

CREATE TABLE investigation_entities (
    investigation_id UUID NOT NULL REFERENCES investigations(id) ON DELETE RESTRICT,
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE RESTRICT,
    depth INTEGER NOT NULL DEFAULT 0 CHECK (depth >= 0),
    inclusion_reason TEXT NOT NULL CHECK (length(btrim(inclusion_reason)) > 0),
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (investigation_id, entity_id)
);

CREATE INDEX idx_investigation_entities_entity ON investigation_entities (entity_id);

-- +goose StatementBegin
CREATE FUNCTION validate_investigation_entity_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    investigation_status TEXT;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'investigation entity membership is append-only';
    END IF;
    SELECT status INTO investigation_status
    FROM investigations
    WHERE id = NEW.investigation_id
    FOR UPDATE;

    IF investigation_status <> 'running' THEN
        RAISE EXCEPTION 'entities may be added only to a running investigation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER investigation_entities_validate_mutation
BEFORE INSERT OR UPDATE OR DELETE ON investigation_entities
FOR EACH ROW EXECUTE FUNCTION validate_investigation_entity_mutation();

-- +goose Down

DROP TABLE investigation_entities;
DROP FUNCTION validate_investigation_entity_mutation();
DROP TRIGGER entity_names_validate_active_link ON entity_names;
DROP TRIGGER entity_identifiers_validate_active_link ON entity_identifiers;
DROP TABLE entity_records;
DROP FUNCTION deactivate_entity_projections_for_link();
DROP FUNCTION validate_active_entity_projection();
DROP FUNCTION validate_entity_record_history();
DROP TABLE match_decisions;
DROP FUNCTION prevent_match_decision_mutation();
DROP FUNCTION apply_match_decision_to_review();
DROP FUNCTION validate_match_decision_insert();
DROP TABLE match_candidates;
DROP FUNCTION validate_match_candidate_mutation();
DROP TABLE match_reviews;
DROP FUNCTION validate_match_review_mutation();
DROP FUNCTION validate_match_review_insert();
DROP FUNCTION assert_record_canonicalization_allowed(UUID);
