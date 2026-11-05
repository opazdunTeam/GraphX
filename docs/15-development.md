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

Нужны Go 1.27, Python 3.15, `uv`, Node.js 24, pnpm 11 и Docker Compose для локальной PostgreSQL 18. Точные версии и допустимые обновления определяются файлами toolchain проекта.

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
uv run ruff check .
uv run mypy
uv run pytest

cd ../apps/web
pnpm check
pnpm build
```
