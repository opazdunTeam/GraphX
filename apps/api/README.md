# GraphX API

Стартовая структура модульного монолита на Go. API владеет HTTP, orchestration, PostgreSQL business state, ingestion результатов workers, entity resolution и read models.

Планируемые энтрипоинты:
- `cmd/api` - Gin HTTP adapter;
- `cmd/consumer` - обработка событий workers;
- внутренние предметные модули располагаются в `internal/<area>`;
- интеграционный код PostgreSQL, RabbitMQ и MinIO остаётся в adapter/platform слоях.

## Запуск и проверка

Все команды ниже выполняются из корня репозитория.

### Windows (PowerShell)

```powershell
# API
cd apps/api
go run ./cmd/api
```

```powershell
# Consumer в отдельном терминале
cd apps/api
go run ./cmd/consumer
```

```powershell
# Проверки
cd apps/api
go test ./...
go vet ./...
```

### Linux

```bash
# API
cd apps/api
go run ./cmd/api
```

```bash
# Consumer в отдельном терминале
cd apps/api
go run ./cmd/consumer
```

```bash
# Проверки
cd apps/api
go test ./...
go vet ./...
```

API предоставляет `/health/live` и `/health/ready`. Пока обязательные внешние зависимости не подключены, readiness возвращает пустой набор checks. Consumer запускается в явно обозначенном idle scaffold mode до реализации RabbitMQ runtime.

См. [архитектуру и структуру репозитория](../../docs/05-architecture.md) и [правила разработки](../../docs/15-development.md).
