# RabbitMQ и фоновые задания

## Зачем нужна очередь

Источники отвечают медленно и непредсказуемо, поэтому их нельзя вызывать внутри обычного HTTP-запроса. RabbitMQ доставляет команды workers и результаты обратно в Go. `source_jobs` при этом хранит бизнес-состояние, которое понимают API и пользователь.

Доставка считается `at least once`: одно сообщение может прийти повторно, и каждый обработчик обязан быть идемпотентным.

## Exchanges и очереди

Для начала достаточно:

- `commands.topic` - команды;
- `events.topic` - результаты и события обработки;
- `dead-letter.topic` - сообщения, которые не удалось обработать;
- `source.collect.q` - обычный сбор;
- `core.events.q` - результаты для Go consumer.

Каждая рабочая очередь имеет DLQ. Очереди `document.parse.q` и `browser.collect.q` добавляются только при выделении соответствующей работы в отдельный процесс.

Publisher отправляет команды в заранее созданный `commands.topic`, используя значение `type` как routing key, например `source.collect.requested.v1`. Очередь `source.collect.q` должна быть привязана к этому ключу; publisher не создаёт exchange, очереди или bindings.

## Формат сообщения

Каждое сообщение содержит:

```json
{
  "message_id": "uuid",
  "type": "source.collect.requested.v1",
  "schema_version": 1,
  "occurred_at": "2026-10-28T12:00:00Z",
  "correlation_id": "investigation-uuid",
  "causation_id": "command-uuid",
  "root_command_id": "root-uuid",
  "producer": "graphx-api",
  "payload": {}
}
```

Точные форматы первой версии находятся в `contracts/schemas/messages`, а ingest-результаты - в `contracts/schemas/ingestion`. Поле вида сообщения во всех схемах называется `type`. Готовые примеры находятся в `contracts/examples`.

Команда сбора самодостаточна: worker получает `job_id`, `attempt_no`, investigation, source, operation, purpose, query, три явных ограничения (`max_records`, `max_bytes`, `timeout_seconds`) и deadline. Ему не требуется доступ к PostgreSQL или локальным значениям лимитов по умолчанию.

`query` имеет общую минимальную форму из `source-query.v1.schema.json`: вид субъекта (`person` или `organization`) и хотя бы исходное имя либо один известный идентификатор. Она совпадает с `SubjectInput` в OpenAPI. Поля, специфичные для источника, не протекают в общий транспорт: adapter выбирает способ запроса по `source` и `operation`, а неподдерживаемую операцию отклоняет стабильной ошибкой.

Допустимые назначения команды: `subject_discovery`, `enrichment` и `branch_expansion`. Повторный разбор сохранённого artifact не является сбором источника и получит отдельный контракт, когда появится соответствующий обработчик.

Результат содержит outcome, `record_count`, ссылки на объекты MinIO или manifest, версии adapter/parser/mapping/normalization, digest и предупреждения. Версия исходной схемы переносится в каждом SourceRecord: это позволяет одному результату содержать записи разных artifacts без дублирования общей версии. Worker назначает переносимые `local_ref` для artifacts и records; после проверки результата Go consumer назначает собственные идентификаторы строк при сохранении данных. Точные поля records и claims определяет ingest-схема.

Есть два взаимоисключающих режима результата:

- inline - `manifest = null`, records и claims находятся в сообщении, а `record_count` равен числу records;
- manifest - ссылка имеет `kind = manifest`, records и claims в сообщении пусты, а `record_count` описывает содержимое manifest по схеме `source-result-manifest.v1.schema.json`.

Manifest содержит собственную версию схемы, `job_id`, `attempt_no`, `record_count`, records и claims. Его `job_id`, `attempt_no` и `record_count` обязаны совпадать с сообщением, а SHA-256 файла - со ссылкой на artifact. Manifest переносит производные данные, но не заменяет исходные материалы: ссылки на них остаются в `artifacts`. Эти проверки выполняет consumer после загрузки объекта.

В обоих режимах сохраняется хотя бы один исходный artifact. Каждый artifact имеет уникальный в пределах результата `local_ref`, а каждый source record указывает свой `artifact_local_ref`. Так Go однозначно создаёт processing run для материала и сохраняет цепочку provenance. `not_found` имеет `record_count = 0`, `manifest = null`, пустые records и claims, но сохраняет исходный ответ источника. Runtime вычисляет `digest` после формирования результата: это SHA-256 канонического JSON тела результата без самого поля `digest`, сериализованного по JSON Canonicalization Scheme. Envelope сообщения в digest не входит. Вход JCS ограничен совместимым с I-JSON числовым доменом: значения, которым нужна точная целочисленная семантика вне безопасного диапазона IEEE 754, передаются строками, что особенно важно для внешних идентификаторов.

До JSON Schema исходные bytes проверяются как однозначный I-JSON/JCS-документ: повторяющиеся имена свойств на любом уровне запрещены. JSON Schema затем проверяет переносимую форму сообщения. После неё worker и Go выполняют семантическую проверку: режим manifest не смешивается с inline-данными, `record_count` согласован с records, claim не пуст, временные даты соответствуют precision, evidence locator указывает на JSON pointer, безопасный положительный номер CSV-строки либо страницу/фрагмент документа, сохраняемые TEXT/JSONB-значения не содержат U+0000, `local_ref` artifacts и records уникальны, а ссылки records на artifacts и ссылки claims на records разрешаются внутри результата. Эти правила остаются кодом доменной границы и не требуют превращать JSON Schema в язык бизнес-логики.

## Жизненный цикл задания

1. Go через transport builder `platform/rabbitmq.PrepareSourceCollectRequestedMessage` формирует и полностью проверяет команду по I-JSON и JSON Schema до передачи в PostgreSQL repository. Builder сверяет `message_id`, `job_id` и `attempt_no` с ожидаемой business identity и возвращает тип с закрытыми полями. Операция из команды сохраняется в `source_jobs.operation`; например, fixture-адаптер поддерживает `search_subject`, а не имя метода SDK `collect`. Затем repository в одной транзакции создаёт `source_job` со статусом `pending`, `attempt_no = 1` и outbox message, повторно сверяя aggregate и attempt с изменяемым job. Repository не интерпретирует JSON payload и отвечает только за его атомарное сохранение с business state. База разрешает такую вставку только для `running` investigation; повторный запуск и переходы к публикации также проверяют lifecycle investigation под блокировкой.
2. Publisher через `FOR UPDATE SKIP LOCKED` арендует одну доступную outbox-запись, отправляет сохранённый payload как persistent message и ждёт publisher confirm. Если процесс остановится до фиксации результата публикации, после истечения аренды другой publisher повторно отправит тот же payload с тем же `message_id`.
3. После положительного confirm outbox помечается опубликованным и аренда снимается. Job переходит `pending -> queued` только условным обновлением: если consumer уже применил итоговый результат, его состояние не меняется.
4. Worker получает команду. Отдельное событие начала работы в обязательный контракт MVP не входит.
5. Worker сохраняет исходные данные и публикует `completed` либо `failed` с классом ошибки. Публикация использует persistent message, mandatory routing и publisher confirm; входная команда подтверждается только после confirm итогового сообщения.
6. Go consumer сначала пытается зарегистрировать `message_id` в inbox, затем под блокировкой проверяет job и номер попытки. Актуальный результат разрешено применять из `pending`, потому что его получение подтверждает публикацию команды.
7. Consumer вычисляет канонический digest payload, создаёт не больше одной записи результата попытки в `source_job_results` для пары job/attempt и связывает принятые artifacts с этой квитанцией. Результат применяется один раз, а job получает итоговый статус.

Только Go меняет `source_jobs`. Publisher обновляет статус через `UPDATE ... WHERE status = 'pending' AND attempt_no = ...`, поэтому не может вернуть уже завершённое задание в `queued` или подтвердить устаревшую попытку. Если investigation уже отменено, outbox всё равно отмечается опубликованным после confirm, но job не переводится в `queued`.

При временной ошибке публикации меняются только данные доставки outbox: увеличивается `publish_attempts`, устанавливается `last_error`, аренда снимается, а `available_at` переносится. Задержка растёт вдвое от 1 секунды до верхнего предела 1 минуты. Состояние `source_jobs` при ошибке публикации не меняется. Это ограничивает частоту повторов и позволяет другим сообщениям продолжать отправляться.

По умолчанию аренда действует 30 секунд, а одна отправка ограничена половиной этого времени. Эти параметры и интервал опроса publisher пока заданы в Go-коде, а не через переменные окружения.

Publisher предоставляет Prometheus/OpenMetrics endpoint `/metrics`. `graphx_outbox_publish_total` считает успешные, отложенные и аварийные обработки, а `graphx_outbox_publish_duration_seconds` измеряет их длительность. При поддержке OpenMetrics в метрику добавляется exemplar с `inv_id`, `job_id` и `msg_id`; эти же идентификаторы есть в структурированном логе события. UUID не используются как labels метрик, чтобы число временных рядов не росло с количеством заданий. Адрес задаётся через `PUBLISHER_METRICS_ADDR`; по умолчанию endpoint доступен только на loopback.

## Типы сообщений

Для MVP зафиксированы только три межкомпонентных сообщения:

- `source.collect.requested.v1`;
- `source.collect.completed.v1`;
- `source.collect.failed.v1`.

Новая попытка использует новую команду `source.collect.requested.v1` с увеличенным `attempt_no`; отдельное сообщение `retry_scheduled` для этого не требуется. Состояние `running` можно добавить через `source.collect.started.v1`, если длительность реальных заданий покажет пользу такого события.

Отдельные `document.parse.*` и `browser.collect.*` не входят в обязательный контракт MVP. Единый Python runtime может загрузить и разобрать документ внутри одного задания источника. Новые типы сообщений появляются только при фактическом выделении отдельного процесса или очереди.

## Идемпотентность

Есть три уровня защиты:

1. `consumed_messages` от повторной доставки одного `message_id`;
2. `source_job_results` с уникальностью `(source_job_id, attempt_no)` от двух сообщений с разными ids для одной попытки;
3. проверки допустимого перехода статуса и актуального `attempt_no`.

## Приём результатов в Go

`cmd/consumer` принимает только `source.collect.completed.v1` и `source.collect.failed.v1` из `events.topic`. Очередь `core.events.q` настроена на отправку отклонённых сообщений в `core.events.dlq` через `dead-letter.topic`. До разбора payload consumer ограничивает размер сообщения, проверяет исходный JSON и соответствующую JSON Schema. Для completed дополнительно сверяется JCS digest payload без поля `digest`; для failed consumer сам рассчитывает JCS digest всего payload. Если completed ссылается на manifest, consumer загружает его из S3/MinIO с отдельным ограничением размера, проверяет схему, размер, SHA-256 и соответствие job, попытки и числа записей.

В одной PostgreSQL-транзакции consumer добавляет `message_id` в `consumed_messages`, блокирует job и investigation, проверяет correlation и `attempt_no`, сохраняет receipt, artifacts и их ссылки. Processing runs, source records и claims создаются только для применимого результата; transport `subject_local_ref` и `object_local_ref` сначала разрешаются в созданные UUID source records. Та же транзакция обновляет business state job. Результат можно принять из `pending`, `queued` или `running`, в том числе если publisher ещё не успел отметить команду как `queued`.

Повторная доставка того же `message_id` не повторяет запись business effect. Другой `message_id` для уже принятой попытки с тем же digest сохраняется как доставка, но не создаёт второй receipt или повторные записи. Отличающийся digest для этой попытки является постоянным конфликтом контракта: транзакция откатывается, сохранённый результат не заменяется, сообщение отправляется в DLQ. Невалидное постоянное сообщение также отправляется в DLQ и не фиксируется как успешно применённое. Для постоянного отказа структурированный лог содержит безопасный `reason_code` и идентификаторы доставки, но не текст сообщения или содержимое payload. Временная ошибка хранения или БД получает NACK с повторной доставкой; ACK отправляется только после успешного commit.

Результат попытки с устаревшим `attempt_no`, завершённой job или уже неработающим investigation сохраняется с `applied = false` и причиной игнорирования. Его artifacts и связи с receipt остаются для аудита, но processing runs, records и claims не создаются. Это же правило не позволяет позднему результату расширить отменённое расследование. Publisher считает сообщение отправленным только после подтверждения от RabbitMQ.

`transient` и `rate_limited` сохраняют `retry_wait` с `next_attempt_at`: базовые задержки составляют 30 секунд, 2 минуты и 10 минут, к ним добавляется случайное отклонение до 25%; `retry_after_seconds` может увеличить задержку, но не более чем до 24 часов. Когда попытки исчерпаны, итоговый статус становится `unavailable`. `action_required` оставляет задание для ручного действия; `failed`, `succeeded`, `not_found` и `unavailable` являются итоговыми состояниями, из которых автоматическое применение результата не возвращает job в работу. Пределы размера задаются `SOURCE_RESULT_TERMINAL_MAX_BYTES` и `SOURCE_RESULT_MANIFEST_MAX_BYTES`; настройки соединений и prefetch используют переменные `RABBITMQ_*` и `S3_*` из `.env.example`.

Python runtime дополнительно объединяет одновременные повторы одной команды. До adapter execution он получает межпроцессный claim по command `message_id`, после claim повторно проверяет journal, а до первой публикации атомарно записывает fingerprint исходной команды и точные bytes terminal message. Claim основан на advisory file lock: ядро автоматически освобождает его после смерти процесса, поэтому другая реплика продолжает работу без stale lease. После acquisition claimant под глобальной блокировкой сверяет inode открытого lock с текущим pathname; waiter на удалённом cleanup inode освобождает stale lock и повторяет acquisition, не допуская двух владельцев разных поколений к adapter execution. После потери confirm или перезапуска переиздаются те же bytes без повторного вызова adapter. Одинаковый command `message_id` с другим fingerprint отвергается.

Файловый backend требует общий durable volume с поддержкой advisory locks, atomic replace и hard links. Default retention terminal journal — 30 суток, конфигурация не принимает окно меньше 24 часов. Завершённые entries и оставшиеся после аварии claim-файлы удаляются только за пределами retention и только если claim свободен. Retention ограничивает возможность точного replay, но не допустимость команды: старый `occurred_at` не доказывает, что команда уже исполнялась. После удаления replay entry повторная доставка снова проходит обычную обработку и deadline-проверку.

Quota ограничивает число entries, общий размер и размер одной записи. Исчерпание общей capacity приводит к NACK с requeue, снятию readiness и остановке consumer с ошибкой: валидная команда остаётся в RabbitMQ, а runtime не создаёт горячий цикл повторов. Если уже построенный terminal result превышает per-entry limit, runtime пытается заменить его компактным `CONTRACT_VALIDATION_FAILED` с реальной версией адаптера и полным списком уже сохранённых artifact references. Частичная provenance не публикуется: если даже компактный failure со всеми ссылками не помещается, применяется тот же capacity-fatal путь, а оператор должен увеличить limit или подключить manifest/receipt contract. Атомарный metadata index хранит агрегированные счётчики и незавершённые reservations, поэтому reserve/commit не сканируют journal; полный обход выполняется только cleanup/reconciliation. Startup log публикует текущие `replay_entries` и `replay_bytes`.

Artifact storage из #11 не заменяет terminal replay journal. Возможный распределённый backend журнала требует отдельного решения, которое сохраняет exact terminal bytes, межпроцессный claim, capacity reservations и безопасный cleanup; обычного S3 PUT для этого недостаточно.

Межпроцессный claim исключает параллельные adapter calls для одной команды, но не превращает внешний HTTP-запрос в exactly-once операцию: смерть владельца после запроса, но до фиксации terminal message допускает последовательный повтор. Поэтому сохранение artifacts обязано использовать детерминированный immutable object key и принимать повторный PUT только при совпадающих фактических bytes, hash и размере. Metadata первого writer остаётся авторитетной и восстанавливается вместе с artifact; повторный запрос не переписывает provenance. Окончательная защита бизнес-эффекта остаётся у Go inbox и уникальности результата попытки.

Если одинаковый `message_id` приходит с другими bytes команды, runtime отклоняет такое сообщение как нарушение transport identity. Это не считается новой попыткой: новая попытка всегда получает новый command `message_id` и увеличенный `attempt_no`.

Для Python worker действует симметричное правило: невалидная команда отклоняется без requeue, потеря соединения или confirm при публикации результата приводит к NACK с requeue, а остановка consumer сначала прекращает новые доставки и не ACK-ает незавершённые. Настройка retry queues, DLQ и исчерпания попыток остаётся отдельной задачей и не дублируется внутри adapter runtime.

`RABBITMQ_PREFETCH_SOURCE` ограничивает число доставленных, но ещё не подтверждённых сообщений, а `WORKER_CONCURRENCY` независимо ограничивает число одновременно выполняемых обработчиков. Effective deadline равен минимуму command deadline, `limits.timeout_seconds` и `WORKER_JOB_TIMEOUT`; в этот единый бюджет входят создание context, adapter и finalization. Shutdown использует отдельный абсолютный deadline, резервирует время на закрытие каналов и повторно отменяет adapter, проигнорировавший первую отмену.

## Повторы и ошибки

Ошибки делятся на:

- временные сетевые;
- rate limit;
- постоянные ошибки запроса или формата;
- `action_required` - например CAPTCHA или ручное действие;
- программные ошибки.

Worker передаёт стабильный `code`, безопасный `message`, `adapter_version`, classification, список уже сохранённых artifacts и при необходимости `retry_after_seconds`. `local_ref` этих artifacts уникальны в пределах failed-результата по тому же правилу, что и для completed. При сетевой ошибке список artifacts может быть пустым; при ошибке формата или валидации уже полученный исходный ответ сохраняется и передаётся вместе с ошибкой. Так объект в MinIO не остаётся без регистрации, сохраняет версию адаптера и может быть повторно разобран после его исправления. Первая версия контракта допускает только коды `SOURCE_TIMEOUT`, `SOURCE_NETWORK_ERROR`, `SOURCE_RATE_LIMITED`, `SOURCE_UNAVAILABLE`, `SOURCE_ACCESS_DENIED`, `SOURCE_ACTION_REQUIRED`, `SOURCE_FORMAT_CHANGED`, `SOURCE_INVALID_RESPONSE`, `SOURCE_UNSUPPORTED_OPERATION`, `CONTRACT_VALIDATION_FAILED` и `INTERNAL_ADAPTER_ERROR`. Runtime получает classification из общего каталога кодов SDK, а не выбирает её отдельно в каждом адаптере. Consumer проверяет согласованность code и classification. Go отображает результат следующим образом:

| Classification    | Действие Go                                                               |
| ----------------- | ------------------------------------------------------------------------- |
| `transient`       | `retry_wait`, после исчерпания попыток - `unavailable`                    |
| `rate_limited`    | `retry_wait` с учётом `retry_after_seconds`                               |
| `permanent`       | `failed`, кроме отдельно обработанного содержательного результата         |
| `action_required` | `action_required` без автоматического повтора                             |
| `bug`             | `failed`, сообщение уходит в наблюдаемость без внутренних деталей для web |

Consumer рассчитывает задержку повтора: 30 секунд, 2 минуты, 10 минут плюс jitter, с учётом `retry_after_seconds`, и транзакционно сохраняет `retry_wait` с `next_attempt_at`. В текущем срезе consumer не запускает следующую попытку и не создаёт для неё outbox-команду: production retry dispatcher отсутствует. Поэтому повторы не выполняются автоматически; запуск новой попытки через `StartNewAttempt` и публикация её команды требуют отдельной реализации. Стратегия доставки повторов через очереди с TTL пока не реализована.

`not_found` - успешный содержательный результат, а не ошибка. Сообщение в DLQ не должно автоматически менять бизнес-статус: оператор или отдельный reconciler решает, можно ли повторить обработку.

## Отмена

Новое задание для отменённого расследования не публикуется. Отдельной команды отмены worker в MVP нет: уже начатый внешний запрос может закончиться по своему deadline. Поздний результат регистрируется как обработанный, но получает `applied = false`; его уже сохранённые artifacts связываются с результатом конкретной попытки, а records, claims, сущности и связи автоматически не создаются. Активная кооперативная отмена добавляется только вместе с отдельным доказанным механизмом доставки cancellation.
