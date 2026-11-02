# GraphX Workers

Единый Python-пакет получения и разбора внешних материалов.

Граница ответственности:

- runtime принимает самодостаточную RabbitMQ-команду;
- adapter получает ответ источника и сначала сохраняет artifact в MinIO;
- parser/mapper возвращают versioned SourceRecord и Claim;
- worker публикует результат или manifest;
- worker не подключается к PostgreSQL и не создаёт канонические Entity/Relationship.

## Использование контрактов

Общие RabbitMQ- и ingest-контракты находятся в [`contracts/schemas`](../contracts/schemas), а полные примеры - в [`contracts/examples`](../contracts/examples). Они задают переносимую границу между Python workers и Go, но не описывают ответы конкретных источников.

Pydantic-модели workers располагаются в `src/graphx_workers/contracts` и пишутся вручную. Генерация этих моделей из JSON Schema в MVP не используется: условия `oneOf`, `if/then/else`, временные правила и произвольные исходные значения дают менее понятный код, чем небольшие явные модели и validators.

При реализации RabbitMQ runtime контракты применяются так:

1. runtime разбирает входную команду `source.collect.requested.v1` в Pydantic-модель;
2. adapter и mapper возвращают `ArtifactReference`, `SourceRecord` и `Claim`, а не произвольные словари;
3. Pydantic проверяет результат до сериализации;
4. контрактный тест сериализует модель и проверяет JSON по соответствующей схеме из корневого `contracts`;
5. готовое сообщение публикуется только после успешного построения модели.

Модели конкретного источника, например сырой ответ ICIJ или строка KASE, находятся внутри каталога этого адаптера. Mapper преобразует их в общие ingest-модели. Расширять общую схему полями одного источника нельзя без общей предметной причины.

Текущие классы в `src/graphx_workers/contracts` являются стартовым каркасом и пока не связаны автоматической проверкой с JSON Schema. Перед включением publisher их нужно согласовать с актуальными схемами и закрепить общими контрактными тестами. Сами schema-файлы не копируются внутрь Python-пакета и остаются единым источником истины в корневом `contracts`.

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
