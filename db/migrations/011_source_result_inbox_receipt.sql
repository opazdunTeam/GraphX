-- +goose Up

ALTER TABLE consumed_messages
    ADD COLUMN source_job_result_id UUID
        REFERENCES source_job_results(id) ON DELETE RESTRICT;

-- +goose Down

ALTER TABLE consumed_messages DROP COLUMN source_job_result_id;
