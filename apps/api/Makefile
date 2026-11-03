.PHONY: fmt test config

CONFIG_DIR := ./internal/platform/config

fmt:
	go fmt ./...

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