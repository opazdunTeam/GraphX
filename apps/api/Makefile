CONFIG_DIR := ./internal/platform/config
DB_URL := $(DATABASE_URL)

.PHONY: fmt test config

fmt:
	go fmt ./...
	go vet ./...

test:
	go test $(dir) -v

test_config:
	$(MAKE) test dir=$(CONFIG_DIR)

	.PHONY: fmt test test-unit test-integration test-db-up test-db-down test-db-logs

test-unit:
	go test ./internal/... -v

test-db-up:
	docker compose -f docker-compose.test.yaml up -d --wait

test-db-down:
	docker compose -f docker-compose.test.yaml down -v

test-db-logs:
	docker compose -f docker-compose.test.yaml logs -f postgres-test

test-integration: test-db-up
	go test -tags=integration ./internal/... -v
	$(MAKE) test-db-down

migrate:
	cd backend && goose -dir ./migrations postgres "$(DB_URL)" up