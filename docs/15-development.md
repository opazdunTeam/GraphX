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
2. Добавить совместимую реализацию producer.
3. Обновить consumer.
4. Переключить использование.
5. Удалить старую версию только после проверки.

Ломающие изменения сообщений получают новую версию routing key или schema. Физическая схема базы меняется только миграцией.

## Начало работы

Нужны Go 1.27, Python 3.15, `uv`, Node.js 24 и pnpm 11. Точные версии и допустимые обновления определяются файлами toolchain проекта.

Команды выполняются из корня репозитория, каждый компонент - в отдельном терминале.

### Windows (PowerShell): запуск

```powershell
cp .env.example .env

cd apps/api
go run ./cmd/api
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
cp .env.example .env

cd apps/api
go run ./cmd/api
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
go test ./...
go vet ./...

cd ../../workers
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
go test ./...
go vet ./...

cd ../../workers
uv run ruff check .
uv run mypy
uv run pytest

cd ../apps/web
pnpm check
pnpm build
```
