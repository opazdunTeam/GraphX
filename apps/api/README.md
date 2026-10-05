# GraphX API

Стартовая структура модульного монолита на Go. API владеет HTTP, orchestration, PostgreSQL business state, ingestion результатов workers, entity resolution и read models.

Планируемые entrypoints:

- `cmd/api` - Gin HTTP adapter;
- `cmd/consumer` - обработка событий workers;
- внутренние предметные модули располагаются в `internal/<area>`;
- интеграционный код PostgreSQL, RabbitMQ и MinIO остаётся в adapter/platform слоях.

Предметный код не должен импортировать Gin, pgx или RabbitMQ SDK.

```powershell
go run ./cmd/api
go run ./cmd/consumer
go test ./...
go vet ./...
```

API предоставляет `/health/live` и `/health/ready`. Пока обязательные внешние зависимости не подключены, readiness возвращает пустой набор checks. Consumer запускается в явно обозначенном idle scaffold mode до реализации RabbitMQ runtime.

См. [структуру репозитория](../../docs/technical/02-repository-structure.md) и [архитектуру](../../docs/technical/01-system-architecture.md).
