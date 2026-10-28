# GraphX

GraphX - платформа для автоматизации due diligence, forensic и corporate intelligence расследований.

## Документация

- [Вся документация проекта](docs/README.md)
- [Концепция и назначение](docs/00-concept.md)
- [Архитектура и структура репозитория](docs/05-architecture.md)
- [Правила разработки](docs/15-development.md)
- [Брендборд проекта](docs/brandboard.md)
- [Лендинг проекта](landing/index.html) ([текущий адрес](http://72.56.109.185))

## Общая структура репозитория

```text
apps/
├── api/          Go API
└── web/          Vue 3 фронтенд
workers/          Python-пакет адаптеров и воркеров для работы с источниками
contracts/        OpenAPI, JSON схема сообщений
db/migrations/    ручные SQL-миграции goose
deploy/           файлы будущего развёртывания
docs/             общая и техническая документация
testdata/         фикстуры для тестов
landing/          статический лендинг для демонстрации дизайна и бренда
```

Каталоги наполняются по мере реализации соответствующей задачи.

## Границы компонентов

- `apps/api` - единственный владелец состояний, хранящихся в PostgreSQL, и канонического графа.
- `workers` - возвращает результаты через RabbitMQ, не подключается к PostgreSQL.
- `apps/web` - использует только публичный HTTP Go API.
- `contracts` - фиксирует форматы на границах компонентов.
- `db/migrations` - единственный источник истины о схеме БД.
- исходные материалы хранятся в S3/MinIO без перезаписи другим содержимым.

## Начало работы

Требуются Go 1.27, Python 3.15, `uv`, Node.js 24 и pnpm 11. `.env.example` содержит перечень переменных; перенесите необходимые значения в локальный `.env`.

Команды выполняются из корня репозитория. API, workers и web запускаются в отдельных терминалах.

### Windows (PowerShell): запуск

```powershell
cp .env.example .env

# Терминал 1: API
cd apps/api
go run ./cmd/api
```

```powershell
# Терминал 2: workers
cd workers
uv sync
uv run graphx-source-worker --check
uv run graphx-document-worker --check
uv run graphx-browser-worker --check
```

```powershell
# Терминал 3: web
cd apps/web
pnpm install --frozen-lockfile
pnpm dev
```

### Linux: запуск

```bash
cp .env.example .env

# Терминал 1: API
cd apps/api
go run ./cmd/api
```

```bash
# Терминал 2: workers
cd workers
uv sync
uv run graphx-source-worker --check
uv run graphx-document-worker --check
uv run graphx-browser-worker --check
```

```bash
# Терминал 3: web
cd apps/web
pnpm install --frozen-lockfile
pnpm dev
```

## Проверка изменений

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
