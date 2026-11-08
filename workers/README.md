# GraphX Workers

Единый Python-пакет получения и разбора внешних материалов.

## Целевая граница ответственности

После подключения RabbitMQ и object storage ответственность распределяется так:

- runtime принимает самодостаточную RabbitMQ-команду;
- adapter получает ответ источника и сначала сохраняет artifact в MinIO;
- parser/mapper возвращают versioned SourceRecord и Claim;
- runtime сохраняет крупный manifest в object storage и публикует completed result: inline либо со ссылкой на manifest;
- worker не подключается к PostgreSQL и не создаёт канонические Entity/Relationship.

## Структура модуля

```text
workers/
├── src/graphx_workers/
│   ├── contracts/       переносимые Pydantic-модели, JSON Schema boundary и семантика
│   ├── sdk/             стабильный API, доступный source adapters
│   ├── runtime/         lifecycle и упаковка результата в transport-сообщение
│   ├── sources/         source-specific client/parser/mapper/metadata/fixtures
│   ├── documents/       общие средства разбора документов
│   ├── normalization/   версионированные общие алгоритмы нормализации
│   ├── entrypoints/     тонкие точки запуска без прикладной логики
│   └── config.py        настройки процесса из окружения
└── tests/               contract, SDK, runtime и entrypoint tests
```

Направление зависимостей:

```text
entrypoints -> runtime -> sdk -> contracts
                     \-> contracts
sources/documents/normalization -> sdk + contracts
```

`contracts` не импортирует runtime, SDK или конкретные adapters. `sdk` не содержит RabbitMQ-,
MinIO- или HTTP-реализаций: он объявляет только модели и capability-протоколы. `runtime`
связывает инфраструктурные реализации с adapters и единолично формирует transport result.
Пакеты конкретных источников не импортируются друг другом.

Владельцы операций:

| Операция | Владелец |
| --- | --- |
| Разбор RabbitMQ envelope и проверка JSON Schema | `runtime` с моделями из `contracts` |
| HTTP-запрос, rate limit, secrets и сохранение artifact | runtime capabilities через `AdapterContext` |
| Интерпретация ответа конкретного источника | `sources/<source>` |
| Построение `SourceRecord` и `Claim` | parser/mapper конкретного источника |
| Inline/manifest выбор, сериализация manifest и JCS digest | `runtime` |
| Стабильные transport error codes | `contracts/errors.py`, экспорт через `sdk` |

`CollectRequest` намеренно не содержит `job_id`, `attempt_no`, `investigation_id`, `source`
или envelope metadata. Runtime выбирает adapter по transport-команде и передаёт ему только
`operation`, `purpose`, `query`, limits и deadline. `CollectResult` также не содержит manifest
или digest: adapter возвращает содержательные artifacts/records/claims, а transport packaging
выполняет runtime после успешного завершения adapter.

## Использование контрактов

Общие RabbitMQ- и ingest-контракты находятся в [`contracts/schemas`](../contracts/schemas), а полные примеры - в [`contracts/examples`](../contracts/examples). Они задают переносимую границу между Python workers и Go, но не описывают ответы конкретных источников.

Pydantic-модели workers располагаются в `src/graphx_workers/contracts` и пишутся вручную. Генерация этих моделей из JSON Schema в MVP не используется: условия `oneOf`, `if/then/else`, временные правила и произвольные исходные значения дают менее понятный код, чем небольшие явные модели и validators.

При реализации RabbitMQ runtime контракты применяются так:

1. runtime разбирает входную команду `source.collect.requested.v1` в Pydantic-модель;
2. runtime преобразует payload в узкий SDK `CollectRequest` и выбирает adapter;
3. adapter и mapper возвращают `ArtifactReference`, `SourceRecord` и `Claim`, а не произвольные словари;
4. runtime проверяет `CollectResult`, выбирает inline/manifest, вычисляет JCS digest и строит transport result;
5. Pydantic и JSON Schema проверяют готовое сообщение перед публикацией;
6. контрактные тесты проверяют сериализацию на общих примерах из корневого `contracts`.

Модели конкретного источника, например сырой ответ ICIJ или строка KASE, находятся внутри каталога этого адаптера. Mapper преобразует их в общие ingest-модели. Расширять общую схему полями одного источника нельзя без общей предметной причины.

Классы в `src/graphx_workers/contracts` согласованы с JSON Schema и проверяются на общих примерах. `ContractSchemaRegistry` предоставляет переносимую проверку Draft 2020-12, которую контрактные тесты уже применяют к исходным JSON bytes и готовым сообщениям; RabbitMQ runtime должен вызывать её перед публикацией. `decode_contract_json` до Schema/Pydantic отвергает duplicate keys, trailing data, non-finite numbers и точные целые вне `±(2^53−1)`. Отдельные semantic validators проверяют ссылки, `record_count`, RFC 8785 digest и manifest. Producer вычисляет digest из окончательно подготовленного transport payload, а consumer проверяет его по исходному JSON до нормализации Pydantic-моделью. Schema-файлы не копируются внутрь Python-пакета и остаются единым источником истины в корневом `contracts`.

Доступны три тонких entrypoint с общим runtime. Это режимы развёртывания одного Python-пакета,
а не три независимых набора контрактов. В MVP обязательны только `source.collect.*`; отдельные
`document.parse.*` и `browser.collect.*` не вводятся, пока для них не появятся собственные очередь
или процессная необходимость. Каталоги `sources`, `documents` и `normalization` остаются без
прикладной реализации до появления первого использующего их вертикального среза.

Все команды ниже выполняются из корня репозитория.

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

Без `--check` entrypoint запускает lifecycle scaffold и ожидает остановки. Подключение RabbitMQ и MinIO добавляется отдельно; до этого процесс явно сообщает scaffold mode и не выдаёт idle-состояние за обработку заданий. Source-specific подпакет с `client`, `parser`, `mapper`, `metadata` и обезличенными fixtures добавляется вместе с первым fixture/real adapter.

См. [контракт адаптера](../docs/10-source-adapters.md) и [сообщения RabbitMQ](../docs/09-messaging.md).
