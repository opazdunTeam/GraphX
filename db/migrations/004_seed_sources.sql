-- +goose Up

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM sources
        WHERE code = 'fixture'
          AND id <> '00000000-0000-4000-8000-000000000001'
    ) OR EXISTS (
        SELECT 1 FROM sources
        WHERE id = '00000000-0000-4000-8000-000000000001'
          AND code <> 'fixture'
    ) THEN
        RAISE EXCEPTION 'fixture source conflicts with the canonical id/code pair';
    END IF;

    INSERT INTO sources (id, code, name, mode, enabled, launch_order)
    VALUES (
        '00000000-0000-4000-8000-000000000001',
        'fixture',
        'Fixture',
        'file',
        TRUE,
        0
    )
    ON CONFLICT (id) DO UPDATE
    SET name = EXCLUDED.name,
        mode = EXCLUDED.mode,
        enabled = EXCLUDED.enabled,
        launch_order = EXCLUDED.launch_order,
        updated_at = NOW();
END;
$$;
-- +goose StatementEnd

-- +goose Down

SELECT 1;
