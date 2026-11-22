# Адаптеры источников

## Одинаковая форма, разное содержимое

У источников разные API, документы, ограничения и ошибки. Адаптер скрывает эти различия и выдаёт общий ingest-контракт. При этом мы не пытаемся запихнуть все исходные поля в одну универсальную таблицу.

Типичный адаптер состоит из:

- `client` - обращение к источнику;
- `parser` - разбор ответа или документа;
- `mapper` - перевод в source records и claims;
- `metadata` - возможности и ограничения;
- `fixtures` - зафиксированные ответы для тестов.

## Контракт

Логически адаптер реализует операцию вроде `collect(request, context)` и объявляет минимальный immutable `AdapterMetadata` первого вертикального среза: source, version и непустой `frozenset` supported operations. Runtime строит регистрацию из metadata самого адаптера, до его вызова отклоняет неподдерживаемую operation и проверяет, что версия успешного результата совпадает с объявленной. Поэтому success и typed failure не могут разойтись по provenance из-за вручную продублированной регистрации. Request содержит запрос, operation, purpose, лимиты и deadline. Общий запрос передаёт вид субъекта и как минимум исходное имя либо известный идентификатор. Адаптер сам переводит их в параметры конкретного источника и дополнительно проверяет source-specific ограничения. Context предоставляет общий HTTP client, rate limiter, artifact storage, логирование, trace, cancellation, progress и доступ к секретам. Runtime также привязывает к context `job_id`, `attempt_no`, `correlation_id` и effective deadline — минимум command deadline, command timeout и worker timeout. Создание context уже входит в этот временной бюджет. Так инфраструктурные capabilities строят детерминированные ключи и operational metadata, не раскрывая adapter transport envelope.

Адаптер не должен самостоятельно создавать подключения к RabbitMQ, MinIO или отдельную систему логов.

Расширенная metadata каталога источников развивается вместе с production registry после первого
среза и должна описывать:

- код и версию адаптера;
- поддерживаемые операции;
- API, файл или browser mode;
- юрисдикции и типы сущностей;
- rate limits и caching;
- необходимость браузера;
- лицензионные и атрибуционные ограничения.

## Результат

Worker обязан сохранить исходный artifact до построения производных данных. Ссылка на artifact содержит локальную ссылку, kind, детерминированный object key, digest, размер, media type, безопасный канонический адрес источника, время получения, известное время публикации и безопасные HTTP-метаданные. `source_uri` допускает только HTTP(S) без userinfo, query и fragment. Это сознательная политика канонизации, а не только фильтр credentials: идентификатор из query адаптер переносит в стабильный path-based URI, если источник определяет такой публичный адрес; signed URL, request parameters и credentials в provenance не переносятся. HTTP metadata использует общий allowlist `etag`, `last-modified`, `content-language`. Версия адаптера передаётся и в успешном результате, и в сообщении об ошибке; при приёме Go записывает её вместе с artifact. Если ошибка возникла после получения материала, например при изменении формата, сохранённые artifacts передаются в `source.collect.failed` и регистрируются без применения производных данных. Если полный terminal result превышает operational replay limit, fallback использует `CONTRACT_VALIDATION_FAILED`, реальную версию адаптера и только полный список artifact references. Частичная регистрация запрещена: если полный failure не помещается, команда возвращается в очередь, а consumer останавливается до исправления capacity или появления отдельного manifest/receipt contract.

Явный порт в `source_uri` записывается каноническим десятичным числом без ведущих нулей в диапазоне `1..65535`; это одинаково проверяют JSON Schema, Python и Go.

Необязательные `original_filename` и `parent_local_ref` сохраняют безопасное исходное имя и происхождение производного материала. Parent разрешается через локальную ссылку того же результата; неизвестные ссылки, self-reference и циклы запрещены.

При будущей регистрации manifest consumer вставляет artifacts в топологическом порядке parent-before-child. Lineage FK намеренно не является deferred: отсутствие parent в том же атомарном результате считается нарушением контракта, а не поводом ослаблять SQL-инвариант.

Source record хранит `local_ref`, `artifact_local_ref`, внешний идентификатор, тип, версию исходной схемы и исходные значения. Ссылка на artifact определяет материал и processing run, из которого получена запись. Все records одного artifact в одном результате обязаны иметь одну `schema_version`; Go отклоняет смешанную форму, потому что она не может однозначно создать versioned processing run. Record должен быть достаточно узким, чтобы его можно было сопоставить с одной сущностью.

Claim атомарен и содержит исходное и нормализованное значения, qualifiers, известные временные границы, их точность и основание, snapshot time, locator и при необходимости короткий excerpt. Locator имеет ровно одну переносимую accessor-форму: RFC 6901 `json_pointer`, положительный `csv_row` либо `page_number` и/или непустой `fragment` документа; рядом допустимы дополнительные source-specific metadata, но произвольный объект без accessor и смешение форм не принимаются. Числовые позиции ограничены точным диапазоном `1..2^53−1`. Версии parser, mapping и normalization передаются один раз на уровне результата; Go записывает их в processing run и созданные claims.

Произвольные значения в `raw_data`, claim values и qualifiers обязаны оставаться совместимыми с I-JSON, JCS и PostgreSQL JSONB: строковые ключи и значения не содержат U+0000. Большие внешние идентификаторы и другие значения, которым нужна точная целочисленная семантика за пределами `±(2^53−1)`, adapter передаёт строками, а не JSON numbers.

Manifest используется, когда результат слишком велик для RabbitMQ. Inline и manifest являются взаимоисключающими режимами: при manifest records и claims не дублируются в сообщении, а `record_count` относится к содержимому manifest. Сам файл соответствует `contracts/schemas/ingestion/source-result-manifest.v1.schema.json`, а ссылка обязана иметь `kind = manifest` и `media_type = application/json`. Исходные artifacts всё равно перечисляются в результате, чтобы records из manifest могли ссылаться на них. Runtime формирует manifest, окончательное сообщение и digest, поэтому adapter возвращает содержательный результат, но не вычисляет transport envelope самостоятельно.

Worker сначала сравнивает сериализованный `SourceResult` с operational-порогом `SOURCE_RESULT_INLINE_MAX_BYTES`, а затем обязательно проверяет полностью подготовленный terminal envelope в точном формате replay journal. Если inline envelope не помещается, успешный результат переводится в manifest mode до записи terminal replay. `SOURCE_RESULT_TERMINAL_MAX_BYTES` отдельно ограничивает payload, который остаётся в RabbitMQ в обоих режимах, и проверяется до материализации JCS digest. Oversized `not_found` не создаёт manifest и завершается typed `CONTRACT_VALIDATION_FAILED`; oversized manifest terminal сохраняет уже записанный manifest в полной failure provenance. Manifest candidate дополнительно проходит точную проверку journal; невозможность сохранить полный terminal result остаётся capacity-fatal, а не приводит к частичной provenance.

Manifest сериализуется один раз как поток компактного JSON в bounded spool immutable storage. Тот же JSON-compatible graph предварительно проверяется общей Schema без повторного декодирования сохранённых bytes; вычисленные storage SHA-256 и размер сверяются при построении результата. `SOURCE_RESULT_MANIFEST_MAX_BYTES` задаёт отдельный верхний предел не более 16 MiB, не ниже inline threshold и не выше `ARTIFACT_MAX_BYTES`: более объёмное исходное содержимое остаётся отдельным immutable artifact, а не переносится в transport records. До reconciliation runtime проверяет логическое окончательное содержимое вместе с recovered artifacts, а после него повторно проверяет точную модель. Длина одного scalar, суммарное scalar-содержимое и число JSON nodes ограничены независимыми `SOURCE_RESULT_MAX_SCALAR_CHARS`, `SOURCE_RESULT_MAX_TOTAL_SCALAR_CHARS` и `SOURCE_RESULT_MAX_JSON_NODES`; aggregate scalar default равен 1 Mi символов. Source-only 64 MiB budget учитывает Unicode storage aggregate-содержимого, худшее escaping одного token, полный JCS buffer terminal payload, in-memory spool, JSON nodes и `WORKER_CONCURRENCY`; document/browser workers этой политикой не ограничиваются. Manifest сохраняется под детерминированной локальной ссылкой; после перезапуска найденный manifest принудительно сохраняет manifest mode и проверяется повторной conditional записью тех же bytes, поэтому retry не может подменить уже опубликованное содержимое.

`save_artifact` получает явный `local_ref` до записи и принимает поток bytes. Runtime строит key из `job_id`, `attempt_no` и безопасно закодированного `local_ref`; исходное имя файла никогда не входит в key. Upload использует conditional PUT без перезаписи. Повтор разрешён только при совпадении SHA-256 и размера, а уже сохранённые references восстанавливаются по префиксу попытки после перезапуска.

Source worker требует `application/json` в `ARTIFACT_ALLOWED_MEDIA_TYPES`: этот тип нужен как runtime-generated manifest и как transport-safe формат JSON-ответов адаптеров. Удалить его конфигурацией и получить поздний `INTERNAL_ADAPTER_ERROR` нельзя; некорректный allowlist отклоняется при старте.

Claim не бывает «пустым». Relationship claim содержит непустой `object_local_ref`; attribute содержит хотя бы одно содержательное исходное или нормализованное значение; event содержит объект либо такое значение. Если источник явно сообщает отсутствие признака, mapper создаёт понятный predicate с булевым либо статусным значением, а не передаёт неразличимый `null`.

## Ошибки

Общие стабильные коды первой версии:

- `SOURCE_TIMEOUT`, `SOURCE_NETWORK_ERROR`, `SOURCE_UNAVAILABLE`;
- `SOURCE_RATE_LIMITED`;
- `SOURCE_ACCESS_DENIED`, включая неверную настройку доступа;
- `SOURCE_ACTION_REQUIRED`, включая CAPTCHA;
- `SOURCE_FORMAT_CHANGED`, `SOURCE_INVALID_RESPONSE`;
- `SOURCE_UNSUPPORTED_OPERATION`;
- `CONTRACT_VALIDATION_FAILED`, `INTERNAL_ADAPTER_ERROR`.

`not_found` передаётся как успешный outcome, а не код ошибки. `SOURCE_FORMAT_CHANGED` нельзя бесконечно повторять: он требует исправления parser.

## Минимальные тесты адаптера

- fixture test без сети;
- parser test на нормальном и пустом ответе;
- mapping test для records и claims;
- проверка transient и permanent errors;
- проверка изменения формата;
- contract test JSON Schema;
- необязательный smoke test реального источника, который запускается отдельно.

Секреты и живые ответы источников не коммитятся. Fixtures должны быть обезличены, если в них есть лишние персональные данные.
