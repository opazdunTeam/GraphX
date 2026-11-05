GOOSE_VERSION := v3.29.0
GOOSE_BUILD_TAGS := no_azuresql no_clickhouse no_libsql no_mssql no_mysql no_sqlite3 no_vertica no_ydb
GOOSE := go run -tags="$(GOOSE_BUILD_TAGS)" github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION)
MIGRATIONS_DIR := ../../db/migrations

.PHONY: fmt vet build test test-race check test-integration \
	test-db-up test-db-down test-db-logs \
	migrate-up migrate-down migrate-status migrate-reset-local migrate-image

fmt:
	gofmt -w ./cmd ./internal

vet:
	go vet ./...

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

check: vet build test

test-integration: test-db-up
	@status=0; \
	POSTGRES_TEST_DSN="postgres://test_user:test_password@localhost:5433/graphx_test?sslmode=disable" \
		go test -tags=integration ./internal/platform/repository/postgres -v || status=$$?; \
	$(MAKE) test-db-down; \
	exit $$status

test-db-up:
	docker compose -f docker-compose.test.yaml up -d --wait

test-db-down:
	docker compose -f docker-compose.test.yaml down -v

test-db-logs:
	docker compose -f docker-compose.test.yaml logs -f postgres-test

migrate-up:
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(POSTGRES_DSN)" up

migrate-down:
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(POSTGRES_DSN)" down

migrate-status:
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(POSTGRES_DSN)" status

migrate-reset-local:
	@test "$(APP_ENV)" = "local" || (echo "APP_ENV must be local" && exit 1)
	@test "$(CONFIRM_RESET)" = "1" || (echo "set CONFIRM_RESET=1" && exit 1)
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(POSTGRES_DSN)" reset

migrate-image:
	docker build -f Dockerfile.migrate -t graphx-migrate ../..
