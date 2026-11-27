# Разработка

## Распределение работы

- RosiaAI - архитектура, контракты, интеграция, web и CI/CD;
- OpazDurov - Python workers, адаптеры, документы, parser и ingestion;
- Sypher - Go API, PostgreSQL, orchestration, entity resolution и связи.

До параллельной работы команда фиксирует только границы: OpenAPI, JSON Schema сообщений, формат manifest, миграции и правила владения данными.

## Порядок первой итерации

Состав первого вертикального среза зафиксирован в [плане MVP](02-mvp-and-plan.md#первый-вертикальный-срез). До начала параллельной работы команда согласует OpenAPI, JSON Schema сообщений, ingest-контракт и первую миграцию. Затем каждый участник реализует свою часть одного и того же сценария, а не отдельный изолированный слой будущей платформы.

После завершения среза на fixture он повторяется с ICIJ, а затем с KASE. Не нужно заранее писать все таблицы, адаптеры или весь интерфейс.

## Порядок изменения контрактов

1. Описать изменение в OpenAPI или JSON Schema.
2. Обновить пример в `contracts/examples` и проверить его по схеме.
3. Перегенерировать Go- и TypeScript-код, если изменился OpenAPI.
4. Добавить совместимую реализацию producer.
5. Обновить consumer.
6. Переключить использование.
7. Удалить старую версию только после проверки.

Сгенерированные файлы фиксируются в репозитории, не редактируются вручную и проверяются повторной генерацией в CI. JSON Schema не генерируют предметную логику или модели базы: они проверяют переносимую форму сообщений. Критические изменения сообщений получают новую версию routing key или schema. Физическая схема базы меняется только миграцией.

## Начало работы

Нужны Go 1.27, Python 3.15, `uv`, Node.js 24, pnpm 11, GNU Make и Docker Compose для локальной PostgreSQL 18. Точные версии и допустимые обновления определяются файлами toolchain проекта.

Команды выполняются из корня репозитория, каждый компонент - в отдельном терминале. Корневой `.env.example` служит каталогом настроек. Значения передаются через shell, Compose или среду запуска; автоматическая загрузка `.env` не является частью runtime.

Для запуска API обязательны доступная PostgreSQL и `POSTGRES_DSN`. Команды ниже используют изолированную тестовую PostgreSQL на `localhost:5433`. После остановки API окружение удаляется командой `docker compose -f docker-compose.test.yaml down -v` из `apps/api`.

### Windows (PowerShell): запуск

```powershell
cd apps/api
$env:POSTGRES_DSN = "postgres://test_user:test_password@localhost:5433/graphx_test?sslmode=disable"
docker compose -f docker-compose.test.yaml up -d --wait
go run ./cmd/api
```

```powershell
# Отдельный publisher process; PostgreSQL и RabbitMQ должны быть доступны
cd apps/api
$env:POSTGRES_DSN = "postgres://test_user:test_password@localhost:5433/graphx_test?sslmode=disable"
$env:RABBITMQ_URL = "amqp://graphx:change-me@localhost:5672/graphx"
go run ./cmd/publisher
```

```powershell
cd workers
uv sync
uv run graphx-source-worker --check
```

```powershell
cd apps/web
pnpm install --frozen-lockfile
pnpm dev
```

### Linux: запуск

```bash
cd apps/api
export POSTGRES_DSN='postgres://test_user:test_password@localhost:5433/graphx_test?sslmode=disable'
docker compose -f docker-compose.test.yaml up -d --wait
go run ./cmd/api
```

```bash
# Отдельный publisher process; PostgreSQL и RabbitMQ должны быть доступны
cd apps/api
export POSTGRES_DSN='postgres://test_user:test_password@localhost:5433/graphx_test?sslmode=disable'
export RABBITMQ_URL='amqp://graphx:change-me@localhost:5672/graphx'
go run ./cmd/publisher
```

```bash
# Проверка RabbitMQ mandatory routing, persistent delivery и publisher confirm
cd apps/api
export RABBITMQ_URL='amqp://graphx:change-me@localhost:5672/graphx'
go test ./internal/platform/rabbitmq -run '^TestConfirmPublisherWithRabbitMQ$' -count=1
```

```bash
cd workers
uv sync
uv run graphx-source-worker --check
```

```bash
cd apps/web
pnpm install --frozen-lockfile
pnpm dev
```

## Базовая проверка

Запускаются только проверки затронутых компонентов. Если тестов для изменения ещё нет, достаточно сборки, линтера и короткой ручной проверки.

### Windows (PowerShell): проверка

```powershell
cd apps/api
make check

cd ../../workers
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest

cd ../apps/web
pnpm check
pnpm build
```

### Linux: проверка

```bash
cd apps/api
make check

cd ../../workers
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest

cd ../apps/web
pnpm check
pnpm build
```

## Обязательный CI

Обычный pull request запускает `.github/workflows/ci.yml`. Jobs `Go`, `Python`, `Web` и `PostgreSQL` независимы и выполняются параллельно; workflow не использует path-фильтры, credentials внешних источников, live source smoke или deployment.

Локальные эквиваленты jobs:

```bash
cd apps/api
make check
make test-race

cd ../../workers
uv sync --locked
uv run --locked ruff format --check .
uv run --locked ruff check .
uv run --locked mypy
uv run --locked pytest
uv run --locked python tools/integrationtest.py

cd ../apps/web
pnpm install --frozen-lockfile
pnpm check
pnpm build

cd ../api
make test-integration
```

В Go job команда `make test-race` получает адрес изолированного RabbitMQ. Интеграционный тест publisher проверяет persistent delivery, ACK, NACK при переполнении очереди, mandatory return без binding и повторную отправку с той же идентичностью. Тест consumer использует `GRAPHX_RESULT_RABBITMQ_TEST_URL` и проверяет повторную доставку при временном сбое, ACK и передачу результата с неверным digest в DLQ.

CI до восстановления кеша проверяет наличие `go.sum`, `uv.lock` и `pnpm-lock.yaml`, а после проверок требует отсутствие tracked-изменений и неожиданных untracked-файлов. Python job поднимает изолированные RabbitMQ 4.2 и MinIO, собирает production worker image через `uv sync --locked`, проводит command→result через сам контейнер и проверяет confirm/ACK, broker NACK, потерю AMQP-соединения до confirm, межпроцессный claim, repeat delivery, concurrency, graceful shutdown, immutable conditional upload, streaming download, conflict и обязательное versioning. После проверки тестовое окружение всегда удаляется. OpenAPI lint входит в `pnpm check`; Go- и TypeScript-генераторы повторно обрабатывают единый `contracts/openapi.yaml` и проверяют ключевую форму generated-типов. Прямой push в `develop` разрешён и запускает полный CI. В `main` изменения попадают только через Pull Request после обязательных checks `Go`, `Python`, `Web` и `PostgreSQL`.
