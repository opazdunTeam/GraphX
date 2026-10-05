# GraphX

GraphX - учебная платформа для автоматизации due diligence, forensic investigation и corporate intelligence. Репозиторий организован как монорепозиторий: Go владеет API, orchestration и канонической моделью, Python - получением и разбором материалов источников, Vue - пользовательским интерфейсом.

## Структура

```text
apps/
├── api/          Go API и consumers
└── web/          Vue 3 SPA
workers/          единый Python-пакет адаптеров и workers
contracts/        OpenAPI и будущие JSON Schema сообщений/ingestion
db/migrations/    ручные SQL-миграции goose
deploy/           необязательные deployment-файлы
docs/             проектные и технические соглашения
testdata/         обезличенные межкомпонентные fixtures
landing/          существующий статический лендинг
```

Каталоги наполняются по мере реализации соответствующей задачи. Пустые слои и сервисы заранее не создаются.

## Границы компонентов

- `apps/api` - единственный владелец PostgreSQL business state и канонического графа.
- `workers` не подключается к PostgreSQL и возвращает результаты через RabbitMQ.
- `apps/web` использует только публичный HTTP API.
- `contracts` фиксирует форматы на границах компонентов.
- `db/migrations` является точным источником истины о реализованной схеме БД.
- исходные материалы хранятся в S3/MinIO без перезаписи другим содержимым.

## Начало работы

Требуются Go 1.26, Python 3.12, `uv`, Node.js 24 и npm 11. Используйте `.env.example` как перечень переменных; реальные значения помещаются в локальный `.env` и не коммитятся.

```powershell
# API
Set-Location apps/api
go run ./cmd/api

# Workers: установка и проверка трёх entrypoints
Set-Location ../../workers
python -m uv sync
python -m uv run graphx-source-worker --check
python -m uv run graphx-document-worker --check
python -m uv run graphx-browser-worker --check

# Web
Set-Location ../apps/web
npm ci
npm run dev
```

Проверка перед pull request:

```powershell
Set-Location apps/api
go test ./...
go vet ./...

Set-Location ../../workers
python -m uv run ruff check .
python -m uv run mypy
python -m uv run pytest

Set-Location ../apps/web
npm run check
npm run build
```

PostgreSQL, RabbitMQ и MinIO подключаются следующими вертикальными задачами. Текущие сервисы не имитируют успешное подключение к отсутствующим зависимостям.


## Документация

- [Концепция и соглашения проекта](docs/project/README.md)
- [Техническая документация](docs/technical/README.md)
- [Структура репозитория](docs/technical/02-repository-structure.md)
- [Правила разработки](docs/technical/15-development-rules.md)
- [Брендборд проекта](docs/brandboard.md)
- [Лендинг проекта](landing/index.html) ([текущий адрес](http://72.56.109.185))
