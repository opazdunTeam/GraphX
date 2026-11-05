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
        ├── api/             Gin, маршруты, middleware и health endpoints
        ├── logging/         настройка структурированных логов
        ├── repository/
        │   └── postgres/    pgx pool и реализации хранилищ
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

Например, решение о следующей попытке задания относится к `orchestration`, а публикация подготовленного сообщения и ожидание publisher confirm - к `platform/rabbitmq`. Интерфейс хранилища объявляется рядом с использующим его предметным модулем, реализация через `pgx` находится в `platform/repository/postgres`, а связывание выполняется в `cmd`.

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
| Новый HTTP handler или middleware           | `internal/platform/api`         |
| SQL-реализация предметного хранилища        | `internal/platform/repository/postgres` |
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

Для этого используется `oapi-codegen`. Конфигурация генератора должна находиться в `apps/api`, а результат - внутри HTTP-адаптера. Точный путь фиксируется задачей подключения генерации. Сгенерированный файл не редактируется вручную. Он фиксируется в репозитории, чтобы изменение HTTP-контракта было видно в pull request, а CI повторяет генерацию и проверяет отсутствие diff.

Реальные handlers остаются в `internal/platform/api` и реализуют сгенерированный интерфейс. Генератор не создаёт прикладную логику, транзакции, SQL, outbox, переходы состояний или read models. Handler преобразует транспортный вызов в команду предметного модуля и возвращает DTO из OpenAPI.

JSON Schema из `contracts/schemas/messages` и `contracts/schemas/ingestion` используются только на RabbitMQ-границе:

- publisher формирует типизированную команду и проверяется контрактным тестом по схеме;
- consumer выбирает схему по полю `type` и проверяет исходный JSON до применения результата;
- после проверки схемы `internal/ingestion` выполняет проверки, зависящие от PostgreSQL и текущего состояния задания.

Go-структуры RabbitMQ и ingest не генерируются из JSON Schema в MVP. Сложные условия схем удобнее выразить в transport-моделях и доменном коде явно, а соответствие контролировать тестами на общих примерах из `contracts/examples`.

Генерация OpenAPI и runtime-проверка JSON Schema пока не подключены. Их следует добавить вместе с первым HTTP handler и RabbitMQ consumer соответственно, а не заранее создавать неиспользуемую инфраструктуру.

## Запуск и проверка

API использует только переменные окружения. Общий каталог переменных проекта и безопасные примеры находятся в корневом [`.env.example`](../../.env.example), но текущий Go API читает только `APP_ENV`, `LOG_LEVEL`, `API_HTTP_ADDR`, `API_*_TIMEOUT`, `POSTGRES_DSN`, `POSTGRES_MAX_CONNS`, `POSTGRES_MIN_CONNS`, `POSTGRES_CONNECT_TIMEOUT` и `POSTGRES_HEALTH_TIMEOUT`. JSON-формат логов пока фиксирован реализацией; `LOG_FORMAT` и остальные будущие настройки из общего файла Go-модуль ещё не использует.

Файл `.env` сам по себе приложением не читается: значения передаются процессу через shell, Compose или среду запуска. `POSTGRES_DSN` обязателен; остальные используемые параметры API и пула имеют проверяемые значения по умолчанию. Для локального smoke-запуска можно использовать тестовую PostgreSQL из `docker-compose.test.yaml`.

Команды ниже выполняются из корня репозитория после передачи переменных окружения процессу.

### Windows (PowerShell)

```powershell
# API
cd apps/api
$env:POSTGRES_DSN = "postgres://test_user:test_password@localhost:5433/graphx_test?sslmode=disable"
docker compose -f docker-compose.test.yaml up -d --wait
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
export POSTGRES_DSN='postgres://test_user:test_password@localhost:5433/graphx_test?sslmode=disable'
docker compose -f docker-compose.test.yaml up -d --wait
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
go test -race ./...
go vet ./...
```

`make check` объединяет переносимые unit tests, vet и build. Усиленная команда `make test-race` запускается отдельно в Linux/CI либо в Windows-среде с `CGO_ENABLED=1` и установленным C-компилятором.

После ручного smoke-запуска тестовая база останавливается и удаляется из `apps/api` командой `docker compose -f docker-compose.test.yaml down -v`.

API предоставляет `/health/live` и `/health/ready` без префикса `/api/v1`. Liveness проверяет только работающий HTTP-процесс. Readiness с коротким timeout проверяет PostgreSQL и возвращает `503 application/problem+json`, если обязательная зависимость недоступна. Бизнес-маршруты находятся под `/api/v1`; до реализации соответствующих use cases их временные handlers честно отвечают `501`.

## PostgreSQL и миграции

API использует прямой `pgxpool.Pool`; GORM и AutoMigrate не применяются. Пул создаётся при старте, проверяется через `Ping` и явно закрывается при остановке процесса. Конкретные repositories получают общий `DBTX` boundary, а многошаговые изменения выполняются через `Store.WithinTransaction`.

Единственный каталог миграций - [`db/migrations`](../../db/migrations). API не запускает миграции при старте. Из `apps/api` доступны отдельные команды:

```bash
make migrate-status
make migrate-up
make migrate-down
```

Сброс предназначен только для пустой локальной учебной БД и требует двух явных подтверждений:

```bash
APP_ENV=local CONFIRM_RESET=1 make migrate-reset-local
```

Версия goose закреплена в Makefile и migration image. Goose собирается только с PostgreSQL-драйвером; ненужные MVP-драйверы других СУБД исключены официальными build tags. Образ собирается из корня репозитория командой `make migrate-image`, запущенной в `apps/api`.

PostgreSQL integration tests отделены build tag `integration` и не требуют БД при обычном `go test ./...`. Команда `make test-integration` поднимает изолированную PostgreSQL 18 на порту `5433`, выполняет pgx-тесты и удаляет тестовый контейнер и volume независимо от результата тестов.

См. [архитектуру и структуру репозитория](../../docs/05-architecture.md) и [правила разработки](../../docs/15-development.md).
