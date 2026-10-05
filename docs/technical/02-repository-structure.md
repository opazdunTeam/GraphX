---
status: draft
owner: tech-lead
reviewers: [project-team]
created: 2026-09-19
updated: 2026-10-02
version: 0.3
---

# Структура репозитория и приложений

## 1. Целевая структура монорепозитория

```text
GraphX/
├── apps/
│   ├── api/                    # Go API
│   └── web/                    # Vue SPA
├── workers/                    # единый Python-пакет воркеров
├── contracts/
│   ├── openapi.yaml
│   ├── asyncapi.yaml           # добавляется после стабилизации сообщений
│   └── schemas/
├── db/
│   ├── migrations/
│   └── seeds/
├── deploy/
│   ├── compose.yaml
│   ├── compose.production.yaml
│   └── proxy/
├── docs/
│   ├── project/
│   ├── technical/
│   └── brandboard.md
├── testdata/                   # обезличенные межкомпонентные фикстуры
├── .github/workflows/
├── .env.example                # безопасный перечень настроек без секретов
└── README.md
```

На момент принятия документа часть каталогов ещё отсутствует. Они создаются по мере начала реализации, а не пустыми заготовками.

`db/queries` не используется: в MVP не применяется `sqlc`, а ручные SQL-запросы располагаются рядом с PostgreSQL-адаптером соответствующего Go-модуля, например в `internal/investigation/postgres.go`. Если запрос становится крупным, его допустимо вынести в соседний файл модуля, не создавая централизованный каталог запросов всей системы.

## 2. Go API

```text
apps/api/
├── cmd/
│   ├── api/main.go
│   └── consumer/main.go        # обработка внутренних событий ядра при необходимости
├── internal/
│   ├── platform/
│   │   ├── config/
│   │   ├── database/
│   │   ├── messaging/
│   │   ├── objectstore/
│   │   ├── logging/
│   │   └── httpserver/
│   ├── investigation/
│   ├── sourcecatalog/
│   ├── ingestion/
│   ├── artifact/
│   ├── claim/
│   ├── entity/
│   ├── matching/
│   ├── relationship/
│   ├── temporal/
│   ├── evidence/
│   ├── graph/
│   └── job/
├── gen/                        # сгенерированные типы, не редактируются вручную
├── tests/
├── Dockerfile
├── go.mod
└── go.sum
```

Рекомендуемое внутреннее устройство модуля:

```text
internal/investigation/
├── domain.go                   # сущности и инварианты
├── service.go                  # прикладные сценарии
├── repository.go              # интерфейс хранилища
├── messages.go                # исходящие/входящие предметные сообщения
├── postgres.go                # адаптер PostgreSQL
├── http.go                    # Gin handlers и DTO mapping
└── service_test.go
```

Это ориентир, а не требование создавать файл каждого типа. Простые модули могут быть компактнее. Предметный слой не импортирует Gin, `pgx`, RabbitMQ SDK или MinIO SDK.

## 3. Python workers

```text
workers/
├── pyproject.toml
├── uv.lock                     # либо lock-файл выбранного менеджера
├── Dockerfile
├── src/graphx_workers/
│   ├── config.py
│   ├── runtime/
│   │   ├── consumer.py
│   │   ├── publisher.py
│   │   ├── idempotency.py
│   │   └── telemetry.py
│   ├── contracts/
│   │   ├── messages.py
│   │   ├── records.py
│   │   ├── claims.py
│   │   └── artifacts.py
│   ├── sdk/
│   │   ├── adapter.py
│   │   ├── fetch.py
│   │   ├── rate_limit.py
│   │   ├── artifact_store.py
│   │   └── errors.py
│   ├── sources/
│   │   ├── icij/
│   │   ├── opensanctions/
│   │   └── opencorporates/
│   ├── documents/
│   │   ├── pdf.py
│   │   ├── html.py
│   │   ├── tables.py
│   │   └── text.py
│   ├── normalization/
│   └── entrypoints/
│       ├── source_worker.py
│       ├── document_worker.py
│       └── browser_worker.py
└── tests/
    ├── fixtures/
    ├── sources/
    └── contracts/
```

Каждый источник изолирован внутри `sources/<source_id>`. Общий код получения, повторов, артефактов и сообщений находится в SDK. Запрещено помещать специфичные для ICIJ условия в общий runtime.

Адаптер источника рекомендуется делить на:

```text
sources/icij/
├── adapter.py
├── client.py
├── parser.py
├── mapper.py
├── models.py
└── tests/
```

## 4. Vue SPA

```text
apps/web/
├── src/
│   ├── app/
│   │   ├── router/
│   │   └── providers/
│   ├── pages/
│   │   ├── investigation-create/
│   │   ├── investigation-progress/
│   │   ├── candidate-review/
│   │   └── investigation-result/
│   ├── features/
│   │   ├── create-investigation/
│   │   ├── review-match/
│   │   ├── expand-branch/
│   │   └── filter-timeline/
│   ├── entities/
│   │   ├── investigation/
│   │   ├── graph/
│   │   ├── evidence/
│   │   └── source-job/
│   ├── shared/
│   │   ├── api/
│   │   ├── ui/
│   │   ├── lib/
│   │   └── types/
│   ├── App.vue
│   └── main.ts
├── public/
├── tests/
├── Dockerfile
├── package.json
└── vite.config.ts
```

Структура используется прагматично: слой создаётся только при наличии кода. Компоненты страниц не должны напрямую разбирать транспортные ответы; преобразование сосредоточено в `shared/api` и моделях сущностей.

## 5. Миграции

- Миграции выполняются утилитой `goose`.
- Файлы пишутся вручную на SQL и содержат секции Up/Down там, где откат безопасен.
- Стартовая миграция создаёт extensions, schemas, enum/check constraints, таблицы, foreign keys и индексы явно.
- GORM `AutoMigrate` и создание схемы при старте API не используются.
- Миграции запускаются отдельным CI/CD step или локальной командой разработчика.

```text
db/migrations/
├── 00001_extensions.sql
├── 00002_sources.sql
├── 00003_investigations_jobs_and_messaging.sql
├── 00004_provenance_and_processing.sql
├── 00005_entities_and_relationships.sql
├── 00006_entity_matching.sql       # этап fuzzy matching
└── 00007_temporal_events.sql       # добавляется после проверки event claims реального источника
```

Разделение стартовой схемы на несколько последовательных файлов упрощает ревью и поиск причины ошибки. Один гигантский файл не является обязательным условием «стартовой миграции».

`00003` включает `source_jobs`, outbox/inbox, бизнес-дедупликацию worker results и HTTP idempotency. `00004` включает независимые artifacts, связи job-artifact, `processing_runs`, source records и claims. Source record уникален внутри processing run, поэтому новый parser не перезаписывает исторический результат. Состав и ограничения этих миграций сначала синхронизируются с [моделью данных](04-data-model.md); сокращённая ERD не заменяет точные `NULL`, `DEFAULT`, `CHECK`, `UNIQUE`, foreign-key и `ON DELETE` правила SQL.

### `db/seeds`

Каталог зарезервирован для необязательных данных, которые нужны только отдельным окружениям или сценариям:

- демонстрационных записей;
- локального наполнения для ручной разработки;
- воспроизводимых наборов для презентации;
- вспомогательных данных, которые не являются обязательной частью схемы.

Для начала разработки `db/seeds` не нужен и не создаётся только ради структуры. Обязательные справочные данные, без которых приложение не может работать, например начальный каталог источников, добавляются идемпотентной версионируемой миграцией в `db/migrations`. Это гарантирует, что они появятся в каждом окружении вместе со схемой.

Seed-файлы запускаются только явной локальной или deployment-командой и должны быть безопасны для повторного выполнения. В них запрещено хранить секреты, реальные расследования и чувствительные персональные данные. Тестовые фикстуры парсеров и контрактов относятся в `testdata/` или каталог тестов, а не в `db/seeds`.

## 6. Контракты и генерация

- OpenAPI хранится в `contracts/openapi.yaml`.
- JSON Schema сообщений - в `contracts/schemas/messages/`.
- JSON Schema результатов адаптеров - в `contracts/schemas/ingestion/`.
- TypeScript API client генерируется в `apps/web/src/shared/api/generated/`.
- Сгенерированный код не изменяется вручную.
- Python Pydantic-модели и Go-модели проверяются контрактными тестами даже при ручной реализации.

## 7. Зависимости между областями

- `apps/web` зависит от публичного HTTP-контракта, но не от внутренней структуры Go.
- `workers` зависит от схем сообщений и ingest-контрактов, но не от таблиц канонического слоя.
- `apps/api` владеет канонической моделью и оркестрацией.
- `db/migrations` изменяются владельцем Go API и проходят ревью технического лидера.
- общая бизнес-логика не выносится в сетевой «shared service» без доказанной необходимости.
