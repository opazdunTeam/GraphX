# GraphX Workers

Единый Python-пакет получения и разбора внешних материалов.

Граница ответственности:

- runtime принимает самодостаточную RabbitMQ-команду;
- adapter получает ответ источника и сначала сохраняет artifact в MinIO;
- parser/mapper возвращают versioned SourceRecord и Claim;
- worker публикует результат или manifest;
- worker не подключается к PostgreSQL и не создаёт канонические Entity/Relationship.

Доступны три entrypoint с общим runtime:

```powershell
python -m uv sync
python -m uv run graphx-source-worker --check
python -m uv run graphx-document-worker --check
python -m uv run graphx-browser-worker --check

python -m uv run ruff check .
python -m uv run mypy
python -m uv run pytest
```

Без `--check` entrypoint запускает lifecycle scaffold и ожидает остановки. Подключение RabbitMQ и MinIO добавляется отдельно; до этого процесс явно сообщает scaffold mode и не выдаёт idle-состояние за обработку заданий. Source-specific каталоги создаются с первым fixture/real adapter.

См. [контракт адаптера](../docs/technical/07-source-adapter-contract.md) и [messaging](../docs/technical/06-messaging.md).
