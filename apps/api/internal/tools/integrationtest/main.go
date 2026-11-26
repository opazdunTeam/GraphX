package main

import (
	"fmt"
	"os"
	"os/exec"
)

const testDSN = "postgres://test_user:test_password@localhost:5433/graphx_test?sslmode=disable"

const (
	gooseVersion = "v3.29.0"
	gooseTags    = "no_azuresql no_clickhouse no_libsql no_mssql no_mysql no_sqlite3 no_vertica no_ydb"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	composeArgs := []string{"compose", "-f", "docker-compose.test.yaml"}
	if err := execute("docker", append(composeArgs, "up", "-d", "--wait")...); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("start integration database: %w", err)
	}
	if err := executePSQL("create extension if not exists pg_trgm"); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("preinstall shared pg_trgm extension: %w", err)
	}
	if err := executeGoose("up-to", "3"); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("apply ingestion database migrations: %w", err)
	}
	if err := expectPSQLFailure(`
		insert into schema_extension_ownership (extension_name) values ('pg_trgm')
	`); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("verify preinstalled extension ownership protection: %w", err)
	}
	if err := executePSQL(`
		insert into sources (id, code, name, mode)
		values ('00000000-0000-4000-8000-000000000099', 'fixture', 'Conflict', 'manual')
	`); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("prepare conflicting fixture source: %w", err)
	}
	if err := executeGoose("up-to", "4"); err == nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("fixture migration unexpectedly accepted a conflicting id/code pair")
	}
	if err := executePSQL("delete from sources where id = '00000000-0000-4000-8000-000000000099'"); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("remove conflicting fixture source: %w", err)
	}
	if err := executeGoose("up-to", "4"); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("apply phase 1 database migrations: %w", err)
	}
	if err := executePSQL(`
		do $$
		begin
			if to_regclass('public.entities') is not null
				or to_regclass('public.match_reviews') is not null
				or not exists (select 1 from pg_extension where extname = 'pg_trgm') then
				raise exception 'version 4 schema boundary is invalid';
			end if;
		end
		$$
	`); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("verify phase 1 schema boundary: %w", err)
	}
	if err := executePSQL(`
		insert into investigations (
			id, subject_type, subject_query, status, stage
		) values (
			'10000000-0000-4000-8000-000000000098', 'person',
			'{"name":"Fixture rollback"}'::jsonb, 'created', 'subject_discovery'
		);
		update investigations set status = 'running'
		where id = '10000000-0000-4000-8000-000000000098';
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			status, request_hash, idempotency_key
		) values (
			'20000000-0000-4000-8000-000000000098',
			'10000000-0000-4000-8000-000000000098',
			'00000000-0000-4000-8000-000000000001',
			'collect', 'subject_discovery', 'pending',
			'fixture-rollback-request', 'fixture-rollback-job'
		)
	`); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("prepare fixture-dependent rollback data: %w", err)
	}
	if err := executeGoose("down-to", "3"); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("roll back fixture migration: %w", err)
	}
	if err := executePSQL(`
		do $$
		begin
			if not exists (
				select 1 from sources
				where id = '00000000-0000-4000-8000-000000000001' and code = 'fixture'
			) then
				raise exception 'fixture source was removed by no-op rollback';
			end if;
		end
		$$
	`); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("verify fixture rollback policy: %w", err)
	}
	if err := executeGoose("up-to", "4"); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("reapply fixture migration: %w", err)
	}
	if err := executeGoose("up-to", "5"); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("apply entity projection migration: %w", err)
	}
	if err := executePSQL(`
		insert into source_job_results (
			id, source_job_id, message_id, attempt_no, result_digest, applied
		) values (
			'30000000-0000-4000-8000-000000000098',
			'20000000-0000-4000-8000-000000000098',
			'30000000-0000-4000-8000-000000000098', 1,
			'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', true
		);
		insert into artifacts (
			id, source_id, kind, media_type, object_key, source_uri,
			sha256, byte_size, retrieved_at, adapter_version
		) values (
			'40000000-0000-4000-8000-000000000098',
			'00000000-0000-4000-8000-000000000001',
			'raw_response', 'application/json', 'migration-boundary/response.json',
			'https://example.test/migration-boundary',
			'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
			2, now(), 'fixture/1'
		);
		insert into source_job_result_artifacts (
			source_job_result_id, artifact_id, artifact_local_ref, role
		) values (
			'30000000-0000-4000-8000-000000000098',
			'40000000-0000-4000-8000-000000000098',
			'boundary-artifact', 'raw_response'
		);
		insert into processing_runs (
			id, artifact_id, source_job_result_id, processing_scope, status,
			schema_version, parser_version, mapping_version, normalization_version, active
		) values (
			'50000000-0000-4000-8000-000000000098',
			'40000000-0000-4000-8000-000000000098',
			'30000000-0000-4000-8000-000000000098',
			'source_collection', 'succeeded', 'schema/1', 'parser/1',
			'mapper/1', 'normalizer/1', true
		);
		insert into source_records (
			id, processing_run_id, local_ref, external_id, record_type, schema_version, raw_data
		) values (
			'60000000-0000-4000-8000-000000000098',
			'50000000-0000-4000-8000-000000000098',
			'boundary-record', 'boundary-1', 'person', 'fixture/1', '{"name":"Boundary"}'::jsonb
		);
		insert into claims (
			id, subject_source_record_id, kind, predicate, raw_value,
			valid_from_precision, valid_to_precision, temporal_basis,
			evidence_locator, parser_version, mapping_version, normalization_version
		) values (
			'70000000-0000-4000-8000-000000000098',
			'60000000-0000-4000-8000-000000000098',
			'attribute', 'name', '"Boundary"'::jsonb,
			'unknown', 'unknown', 'unknown', '{"pointer":"/name"}'::jsonb,
			'parser/1', 'mapper/1', 'normalizer/1'
		);
		insert into entities (id, entity_type, display_name)
		values ('80000000-0000-4000-8000-000000000098', 'person', 'Boundary');
		insert into entity_names (
			id, entity_id, claim_id, raw_name, normalized_name, normalization_version
		) values (
			'81000000-0000-4000-8000-000000000098',
			'80000000-0000-4000-8000-000000000098',
			'70000000-0000-4000-8000-000000000098',
			'Boundary', 'boundary', 'normalizer/1'
		);
		insert into entity_identifiers (
			id, entity_id, claim_id, identifier_type,
			raw_value, normalized_value, normalization_version
		) values (
			'82000000-0000-4000-8000-000000000098',
			'80000000-0000-4000-8000-000000000098',
			'70000000-0000-4000-8000-000000000098',
			'test', 'Boundary', 'boundary', 'normalizer/1'
		)
	`); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("prepare active projection at version 5: %w", err)
	}
	if err := executeGoose("up-to", "6"); err == nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("migration 006 unexpectedly accepted active projections without links")
	}
	if err := executePSQL(`
		do $$
		begin
			if (select max(version_id) from goose_db_version where is_applied) <> 5
				or to_regclass('public.match_reviews') is not null
				or not exists (select 1 from entity_names where active)
				or not exists (select 1 from entity_identifiers where active) then
				raise exception 'failed migration 006 did not roll back cleanly to version 5';
			end if;
		end
		$$;
		update entity_names set active = false
		where id = '81000000-0000-4000-8000-000000000098';
		update entity_identifiers set active = false
		where id = '82000000-0000-4000-8000-000000000098'
	`); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("verify and deactivate rejected version 5 projections: %w", err)
	}
	if err := executeGoose("up-to", "6"); err == nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("migration 006 unexpectedly accepted inactive projections without links")
	}
	if err := executePSQL(`
		do $$
		begin
			if (select max(version_id) from goose_db_version where is_applied) <> 5
				or to_regclass('public.match_reviews') is not null
				or (select count(*) from entity_names) <> 1
				or (select count(*) from entity_identifiers) <> 1
				or exists (select 1 from entity_names where active)
				or exists (select 1 from entity_identifiers where active) then
				raise exception 'inactive orphan projections were not rejected transactionally';
			end if;
		end
		$$
	`); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("verify rejection of inactive version 5 projections: %w", err)
	}
	if err := executeGoose("down-to", "4"); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("remove rejected version 5 projection schema: %w", err)
	}
	if err := executeGoose("up-to", "5"); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("reapply clean entity projection migration: %w", err)
	}
	if err := executePSQL(`
		do $$
		begin
			if exists (select 1 from entity_names)
				or exists (select 1 from entity_identifiers) then
				raise exception 'clean version 5 contains projection history';
			end if;
		end
		$$
	`); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("verify clean entity projection boundary: %w", err)
	}
	if err := executeGoose("up-to", "6"); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("apply clean entity resolution boundary: %w", err)
	}
	if err := executePSQL(`
		do $$
		begin
			if to_regclass('public.entities') is null
				or to_regclass('public.match_reviews') is null
				or to_regclass('public.relationships') is not null
				or to_regclass('public.events') is not null then
				raise exception 'version 6 schema boundary is invalid';
			end if;
		end
		$$
	`); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("verify entity resolution schema boundary: %w", err)
	}
	if err := executeGoose("up-to", "7"); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("apply artifact lineage migration: %w", err)
	}
	if err := executePSQL(`
		do $$
		begin
			if (select max(version_id) from goose_db_version where is_applied) <> 7
				or not exists (
					select 1 from information_schema.columns
					where table_name = 'artifacts' and column_name = 'original_filename'
				)
				or not exists (
					select 1 from information_schema.columns
					where table_name = 'artifacts' and column_name = 'parent_artifact_id'
				) then
				raise exception 'version 7 artifact lineage schema is invalid';
			end if;
		end
		$$
	`); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("verify artifact lineage migration: %w", err)
	}
	if err := executeGoose("up-to", "8"); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("apply transactional outbox lease migration: %w", err)
	}
	if err := executePSQL(`
		do $$
		begin
			if (select max(version_id) from goose_db_version where is_applied) <> 8
				or not exists (
					select 1 from information_schema.columns
					where table_name = 'outbox_messages' and column_name = 'lease_token'
				)
				or not exists (
					select 1 from information_schema.columns
					where table_name = 'outbox_messages' and column_name = 'lease_until'
				) then
				raise exception 'version 8 outbox lease schema is invalid';
			end if;
		end
		$$
	`); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("verify transactional outbox lease migration: %w", err)
	}
	if err := executeGoose("up-to", "9"); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("apply investigation creation migration: %w", err)
	}
	if err := executePSQL(`
		do $$
		begin
			if (select max(version_id) from goose_db_version where is_applied) <> 9
				or not exists (
					select 1 from information_schema.columns
					where table_name = 'investigations' and column_name = 'raw_subject'
				)
				or not exists (
					select 1 from information_schema.columns
					where table_name = 'investigations' and column_name = 'normalized_subject'
				)
				or (select column_default from information_schema.columns
					where table_name = 'investigations' and column_name = 'state_version') not in ('1', '1::bigint')
			then
				raise exception 'version 9 investigation creation schema is invalid';
			end if;
		end
		$$
	`); err != nil {
		_ = execute("docker", append(composeArgs, "down", "-v")...)
		return fmt.Errorf("verify investigation creation migration: %w", err)
	}

	testCommand := exec.Command(
		"go",
		"test",
		"-count=1",
		"-tags=integration",
		"./internal/platform/repository/postgres",
		"-v",
	)
	testCommand.Env = append(os.Environ(), "POSTGRES_TEST_DSN="+testDSN)
	testCommand.Stdout = os.Stdout
	testCommand.Stderr = os.Stderr
	testErr := testCommand.Run()
	resetErr := executeGoose("reset")
	preinstalledExtensionErr := executePSQL(`
		do $$
		begin
			if not exists (select 1 from pg_extension where extname = 'pg_trgm') then
				raise exception 'pg_trgm was removed by migration rollback';
			end if;
		end
		$$
	`)
	var ownedExtensionErr error
	if resetErr == nil && preinstalledExtensionErr == nil {
		if err := executePSQL("drop extension pg_trgm"); err != nil {
			ownedExtensionErr = fmt.Errorf("prepare clean extension ownership path: %w", err)
		} else if err := executeGoose("up-to", "1"); err != nil {
			ownedExtensionErr = fmt.Errorf("create migration-owned pg_trgm extension: %w", err)
		} else if err := executePSQL(`
			do $$
			begin
				if not exists (select 1 from pg_extension where extname = 'pg_trgm')
					or not exists (
						select 1 from schema_extension_ownership where extension_name = 'pg_trgm'
					) then
					raise exception 'migration did not record pg_trgm ownership';
				end if;
			end
			$$
		`); err != nil {
			ownedExtensionErr = fmt.Errorf("verify migration-owned pg_trgm extension: %w", err)
		} else if err := expectPSQLFailure(`
			delete from schema_extension_ownership where extension_name = 'pg_trgm'
		`); err != nil {
			ownedExtensionErr = fmt.Errorf("verify migration-owned marker protection: %w", err)
		} else if err := expectPSQLFailure(`
			update schema_extension_ownership set created_at = now()
			where extension_name = 'pg_trgm'
		`); err != nil {
			ownedExtensionErr = fmt.Errorf("verify migration-owned marker update protection: %w", err)
		} else if err := executeGoose("reset"); err != nil {
			ownedExtensionErr = fmt.Errorf("roll back migration-owned pg_trgm extension: %w", err)
		} else if err := executePSQL(`
			do $$
			begin
				if exists (select 1 from pg_extension where extname = 'pg_trgm') then
					raise exception 'migration-owned pg_trgm survived rollback';
				end if;
			end
			$$
		`); err != nil {
			ownedExtensionErr = fmt.Errorf("verify migration-owned pg_trgm cleanup: %w", err)
		}
	}
	downErr := execute("docker", append(composeArgs, "down", "-v")...)

	if testErr != nil {
		return fmt.Errorf("run integration tests: %w", testErr)
	}
	if resetErr != nil {
		return fmt.Errorf("roll back database migrations: %w", resetErr)
	}
	if preinstalledExtensionErr != nil {
		return fmt.Errorf("verify preinstalled pg_trgm extension after rollback: %w", preinstalledExtensionErr)
	}
	if ownedExtensionErr != nil {
		return ownedExtensionErr
	}
	if downErr != nil {
		return fmt.Errorf("remove integration database: %w", downErr)
	}
	return nil
}

func executePSQL(statement string) error {
	return execute(
		"docker", "exec", "graphx-postgres-test",
		"psql", "-v", "ON_ERROR_STOP=1", "-U", "test_user", "-d", "graphx_test", "-c", statement,
	)
}

func expectPSQLFailure(statement string) error {
	if err := executePSQL(statement); err == nil {
		return fmt.Errorf("statement unexpectedly succeeded")
	}
	return nil
}

func executeGoose(command ...string) error {
	gooseArgs := []string{
		"run",
		"-tags=" + gooseTags,
		"github.com/pressly/goose/v3/cmd/goose@" + gooseVersion,
		"-dir",
		"../../db/migrations",
		"postgres",
		testDSN,
	}
	gooseArgs = append(gooseArgs, command...)
	return execute("go", gooseArgs...)
}

func execute(name string, args ...string) error {
	command := exec.Command(name, args...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	return command.Run()
}
