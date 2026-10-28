# GraphX Workers

Единый Python-пакет получения и разбора внешних материалов.

Граница ответственности:

- runtime принимает самодостаточную RabbitMQ-команду;
- adapter получает ответ источника и сначала сохраняет artifact в MinIO;
- parser/mapper возвращают versioned SourceRecord и Claim;
- worker публикует результат или manifest;
- worker не подключается к PostgreSQL и не создаёт канонические Entity/Relationship.

Доступны три entrypoint с общим runtime. Все команды ниже выполняются из корня репозитория.

## Запуск и проверка

### Windows (PowerShell)

```powershell
cd workers
uv sync
uv run graphx-source-worker --check
uv run graphx-document-worker --check
uv run graphx-browser-worker --check

uv run ruff check .
uv run mypy
uv run pytest
```

### Linux

```bash
cd workers
uv sync
uv run graphx-source-worker --check
uv run graphx-document-worker --check
uv run graphx-browser-worker --check

uv run ruff check .
uv run mypy
uv run pytest
```

Без `--check` entrypoint запускает lifecycle scaffold и ожидает остановки. Подключение RabbitMQ и MinIO добавляется отдельно; до этого процесс явно сообщает scaffold mode и не выдаёт idle-состояние за обработку заданий. Source-specific каталоги создаются с первым fixture/real adapter.

См. [контракт адаптера](../docs/10-source-adapters.md) и [сообщения RabbitMQ](../docs/09-messaging.md).
