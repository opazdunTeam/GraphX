# GraphX API

Стартовая структура модульного монолита на Go. API владеет HTTP, orchestration, PostgreSQL business state, ingestion результатов workers, entity resolution и read models.

## Архитектура каталогов

Целевая структура Go-приложения:

```text
apps/api/
├── cmd/
│   ├── api/                 запуск HTTP API
│   └── consumer/            обработка результатов workers
└── internal/
    ├── investigations/      жизненный цикл и этапы расследования
    ├── orchestration/       source jobs, повторы и частичный результат
    ├── ingestion/           приём artifacts, records и claims
    ├── resolution/          кандидаты, reviews и решения о совпадениях
    ├── knowledge/           entities, claims и фактические relationships
    ├── readmodels/          status, evidence, graph и timeline DTO
    └── platform/
        ├── config/          чтение и проверка настроек
        ├── httpserver/      Gin, маршруты, middleware и health endpoints
        ├── logging/         настройка структурированных логов
        ├── postgres/        реализации хранилищ через pgx
        ├── rabbitmq/        publisher, consumer и transport concerns
        └── objectstore/     работа с MinIO/S3
```

Каталоги добавляются по мере реализации соответствующего модуля.

### Точки запуска

- `cmd/api` собирает зависимости и запускает HTTP-сервер;
- `cmd/consumer` собирает зависимости и принимает результаты workers из RabbitMQ;

### Предметные модули

- `investigations` владеет состояниями и этапами расследования;
- `orchestration` владеет `source_jobs`, планированием попыток, частичным результатом и правилами повторов;
- `ingestion` проверяет результат worker и преобразует переносимые records и claims в сохраняемые данные;
- `resolution` формирует кандидатов, reviews и обратимые решения о сопоставлении;
- `knowledge` владеет каноническими сущностями, утверждениями и фактическими связями;
- `readmodels` собирает DTO для API.

### Platform

`platform` содержит технические адаптеры и не владеет бизнес-правилами. Здесь находятся детали Gin, `pgx`, RabbitMQ, MinIO, конфигурации и логирования.

Например, решение о следующей попытке задания относится к `orchestration`, а публикация подготовленного сообщения и ожидание publisher confirm - к `platform/rabbitmq`. Интерфейс хранилища объявляется рядом с использующим его предметным модулем, реализация через `pgx` находится в `platform/postgres`, а связывание выполняется в `cmd`.

### Правила зависимостей

- предметные модули не импортируют Gin, `pgx`, RabbitMQ SDK или MinIO SDK;
- `platform` может импортировать предметные модули для реализации их интерфейсов;
- `cmd` может импортировать и модули, и `platform`, потому что собирает приложение;
- один модуль не меняет таблицы и состояние другого в обход его операций;
- межмодульный вызов проходит через явно названный сервис или интерфейс;
- каталоги `common`, `shared`, `helpers` и `utils` не создаются без конкретной общей ответственности.

## Куда добавлять код

| Задача                                      | Каталог                         |
| ------------------------------------------- | ------------------------------- |
| Новый HTTP handler или middleware           | `internal/platform/httpserver`  |
| SQL-реализация предметного хранилища        | `internal/platform/postgres`    |
| Публикация или получение RabbitMQ-сообщения | `internal/platform/rabbitmq`    |
| Работа с объектом MinIO                     | `internal/platform/objectstore` |
| Переход состояния расследования             | `internal/investigations`       |
| Retry и состояние source job                | `internal/orchestration`        |
| Проверка ingest-результата                  | `internal/ingestion`            |
| Решение о совпадении записей                | `internal/resolution`           |
| Создание entity или relationship            | `internal/knowledge`            |
| Сборка status, evidence, graph или timeline | `internal/readmodels`           |

## Использование контрактов

Источником HTTP-контракта служит [`contracts/openapi.yaml`](../../contracts/openapi.yaml). При реализации первого endpoint из него нужно генерировать:

- Go-типы HTTP-запросов и ответов;
- серверный интерфейс операций;
- Gin-адаптер, который разбирает параметры и вызывает реализацию интерфейса.

Для этого используется `oapi-codegen`. Конфигурация генератора должна находиться в `apps/api`, а результат - в `internal/platform/httpserver/generated/openapi.gen.go`. Сгенерированный файл не редактируется вручную. Он фиксируется в репозитории, чтобы изменение HTTP-контракта было видно в pull request, а CI повторяет генерацию и проверяет отсутствие diff.

Реальные handlers остаются в `internal/platform/httpserver` и реализуют сгенерированный интерфейс. Генератор не создаёт прикладную логику, транзакции, SQL, outbox, переходы состояний или read models. Handler преобразует транспортный вызов в команду предметного модуля и возвращает DTO из OpenAPI.

JSON Schema из `contracts/schemas/messages` и `contracts/schemas/ingestion` используются только на RabbitMQ-границе:

- publisher формирует типизированную команду и проверяется контрактным тестом по схеме;
- consumer выбирает схему по полю `type` и проверяет исходный JSON до применения результата;
- после проверки схемы `internal/ingestion` выполняет проверки, зависящие от PostgreSQL и текущего состояния задания.

Go-структуры RabbitMQ и ingest не генерируются из JSON Schema в MVP. Сложные условия схем удобнее выразить в transport-моделях и доменном коде явно, а соответствие контролировать тестами на общих примерах из `contracts/examples`.

Генерация OpenAPI и runtime-проверка JSON Schema пока не подключены. Их следует добавить вместе с первым HTTP handler и RabbitMQ consumer соответственно, а не заранее создавать неиспользуемую инфраструктуру.

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
