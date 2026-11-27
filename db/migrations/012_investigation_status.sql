-- +goose Up

UPDATE investigations SET state_version = 1 WHERE state_version = 0;
ALTER TABLE investigations ADD CONSTRAINT investigations_positive_state_version CHECK (state_version >= 1);

ALTER TABLE source_jobs ADD COLUMN last_error_at TIMESTAMPTZ;
UPDATE source_jobs job
SET last_error_at = COALESCE((
    SELECT result.received_at
    FROM source_job_results result
    WHERE result.source_job_id = job.id AND result.applied
      AND job.status IN ('pending', 'queued', 'running', 'cancelled')
      AND result.attempt_no BETWEEN job.attempt_no - 1
          AND
          CASE WHEN job.status = 'cancelled' THEN job.attempt_no ELSE job.attempt_no - 1 END
    ORDER BY result.attempt_no DESC
    LIMIT 1
), job.updated_at)
WHERE job.last_error_code IS NOT NULL OR job.last_error_message IS NOT NULL;

-- +goose StatementBegin
CREATE FUNCTION track_source_job_error_time()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.last_error_code IS NULL AND NEW.last_error_message IS NULL THEN
        NEW.last_error_at := NULL;
    ELSIF TG_OP = 'INSERT' THEN
        NEW.last_error_at := statement_timestamp();
    ELSIF NEW.last_error_code IS DISTINCT FROM OLD.last_error_code
        OR NEW.last_error_message IS DISTINCT FROM OLD.last_error_message
        OR (NEW.status IS DISTINCT FROM OLD.status
            AND NEW.status IN ('retry_wait', 'action_required', 'unavailable', 'failed')) THEN
        NEW.last_error_at := statement_timestamp();
    ELSE
        NEW.last_error_at := OLD.last_error_at;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER source_jobs_error_time
BEFORE INSERT OR UPDATE ON source_jobs
FOR EACH ROW EXECUTE FUNCTION track_source_job_error_time();

-- +goose Down

DROP TRIGGER source_jobs_error_time ON source_jobs;
DROP FUNCTION track_source_job_error_time();
ALTER TABLE source_jobs DROP COLUMN last_error_at;
ALTER TABLE investigations DROP CONSTRAINT investigations_positive_state_version;
