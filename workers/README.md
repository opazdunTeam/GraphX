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

`AdapterContext.logger` предназначен только для скалярных operational metadata из явного allowlist; остальные context-поля отбрасываются, а structured values закрываются целиком. Адаптер не передаёт в сообщение или structured context raw payload, HTTP headers/body, credentials, URL/URI, полные документы и лишние персональные данные; исходный ответ сохраняется через `save_artifact`.

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

uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest
uv run python tools/integrationtest.py
```

### Linux

```bash
cd workers
uv sync
uv run graphx-source-worker --check
uv run graphx-document-worker --check
uv run graphx-browser-worker --check

uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest
uv run python tools/integrationtest.py
```

Без `--check` source entrypoint требует `RABBITMQ_URL`, объявляет `commands.topic`, `events.topic` и `source.collect.q`, применяет `RABBITMQ_PREFETCH_SOURCE`, ограничивает активные обработчики через `WORKER_CONCURRENCY` и запускает manual-ACK consumer. Runtime проверяет входные и итоговые bytes по общим JSON Schema, передаёт adapter только SDK context, атомарно сохраняет exact terminal bytes в `TERMINAL_REPLAY_ROOT`, публикует их с publisher confirm и лишь затем подтверждает команду. При shutdown новые доставки останавливаются, выполняющиеся задания получают ограниченное время на завершение, после чего повторно отменяются без ACK, а оставшееся время используется для закрытия transport.

`TERMINAL_REPLAY_ROOT` должен находиться на durable volume, общем для реплик одной consumer group и поддерживающем advisory file locks и atomic hard links. Межпроцессный claim берётся до adapter execution и автоматически освобождается ядром после смерти владельца. После acquisition runtime сверяет inode открытого lock с актуальным pathname, поэтому waiter на удалённом cleanup inode не может работать параллельно с владельцем вновь созданного lock. Journal сохраняется до первой публикации и обеспечивает точное переиздание результата после потери confirm или перезапуска.

`TERMINAL_REPLAY_RETENTION` по умолчанию равен 30 суткам и не может быть меньше суток. Cleanup удаляет только entries за пределами окна со свободным claim; возраст команды сам по себе не запрещает обработку. Настройки `TERMINAL_REPLAY_MAX_ENTRIES`, `TERMINAL_REPLAY_MAX_BYTES` и `TERMINAL_REPLAY_MAX_ENTRY_BYTES` ограничивают journal. Общая нехватка capacity возвращает delivery в очередь и останавливает consumer с ошибкой, чтобы команда не потерялась и не образовала горячий retry loop. Oversized terminal заменяется компактным `CONTRACT_VALIDATION_FAILED` только если в него помещаются реальная версия адаптера и все сохранённые artifacts; частичный список не публикуется, а невозможность сохранить полный failure переводит runtime в capacity-fatal режим. Hot path использует атомарный metadata index, а полный обход каталога выполняется только при cleanup/reconciliation. Journal не заменяет immutable artifact storage: смерть процесса после внешнего запроса, но до terminal result допускает последовательный повтор, поэтому artifact key и проверка hash остаются обязательной частью #11.

Immutable storage использует key `artifacts/v1/{job_id}/{attempt_no}/{encoded-local-ref}` и conditional S3 PUT. Content сначала проходит bounded spool для SHA-256, размера и byte limit, затем передаётся клиенту потоком. Повтор того же key успешен только при совпадении фактических bytes, hash и размера; metadata первого writer остаётся авторитетной, другое содержимое даёт conflict без перезаписи. Участок PUT/reconciliation/регистрации cancellation-safe: daemon-операция владеет spool до прекращения чтения, а первая отмена получает короткое bounded drain-окно для регистрации подтверждённого artifact. Истечение окна или повторная отмена дают uncertain outcome и NACK без terminal result; worker shutdown не ждёт зависший SDK-поток. `source_uri` передаётся как стабильный канонический HTTP(S) URI, включая bracketed IPv6, но без userinfo, query и fragment: resource identifiers из query адаптер обязан перенести в безопасный path-based canonical URI либо не сохранять такой адрес как provenance. HTTP metadata ограничена безопасным allowlist. `original_filename` хранится только как очищенная metadata, а parent задаётся локальной ссылкой. При восстановлении session перечисляет префикс попытки, потоково проверяет фактический SHA-256 каждого объекта и включает найденные references в полный terminal result. Download доступен только для точной ссылки, зарегистрированной текущей session, и выдаёт данные после проверки целостности через bounded spool.

GET/read/close выполняются одним владеющим daemon worker и передают chunks через bounded очередь; cancellation прекращает потребление, но не закрывает body параллельно с `read`. `S3_MAX_INFLIGHT_OPERATIONS` ограничивает все живые SDK operations, включая detached вызовы, и при настроенном storage не может быть ниже `WORKER_CONCURRENCY`. Slot освобождается только после фактического завершения blocking lifecycle; исчерпание лимита и постоянные ошибки конфигурации object store приводят к NACK и остановке consumer, а не к adapter terminal result или hot requeue loop. Подтверждённая infrastructure failure имеет приоритет над одновременной cancellation/deadline.

До задачи fixture-адаптера registry источников пуст: валидная команда получает стабильный `SOURCE_UNSUPPORTED_OPERATION`, а не остаётся незавершённой. Immutable artifact storage и manifest finalizer подключаются отдельным инфраструктурным срезом; transport runtime не получает доступ к PostgreSQL. Document и browser entrypoints пока остаются явными scaffold-процессами.

Production image собирается из корня репозитория, чтобы сохранить единый источник JSON Schema:

```bash
docker build -f workers/Dockerfile .
```

Production image устанавливает runtime и все транзитивные зависимости через `uv sync --locked --no-dev` из committed `uv.lock`. `tools/integrationtest.py` поднимает изолированные RabbitMQ и MinIO, собирает этот image, проводит через контейнер реальный command→failed-result flow, а затем проверяет confirm/ACK, broker NACK, физический разрыв AMQP-соединения до confirm, concurrency, graceful shutdown, immutable streaming upload/download, concurrent retry, conflict, recovery и обязательное versioning. После проверки контейнеры, сеть и volume удаляются. Обычный `pytest` пропускает infrastructure-тесты без `RABBITMQ_TEST_URL` и `MINIO_TEST_URL`.

См. [контракт адаптера](../docs/10-source-adapters.md) и [сообщения RabbitMQ](../docs/09-messaging.md).
