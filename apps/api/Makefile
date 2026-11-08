GOOSE_VERSION := v3.29.0
GOOSE_BUILD_TAGS := no_azuresql no_clickhouse no_libsql no_mssql no_mysql no_sqlite3 no_vertica no_ydb
GOOSE := go run -tags="$(GOOSE_BUILD_TAGS)" github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION)
MIGRATIONS_DIR := ../../db/migrations
OAPI_CODEGEN_VERSION := v2.8.2
OPENAPI_SPEC := ../../contracts/openapi.yaml
OPENAPI_CONFIG := oapi-codegen.yaml
OPENAPI_OUTPUT := internal/platform/api/generated/openapi.gen.go

.PHONY: fmt vet build test test-race check check-contracts generate-api check-api test-integration \
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

generate-api:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION) --config $(OPENAPI_CONFIG) --o $(OPENAPI_OUTPUT) $(OPENAPI_SPEC)

check-api:
	go run ./internal/tools/openapicheck

check-contracts:
	go run ./internal/tools/contractcheck

check: check-api check-contracts vet build test

test-integration:
	go run ./internal/tools/integrationtest

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
