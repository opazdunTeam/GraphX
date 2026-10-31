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
- `document.parse.q` - разбор документов;
- `browser.collect.q` - только для источников, которым действительно нужен браузер;
- `core.events.q` - результаты для Go consumer.

Каждая рабочая очередь имеет DLQ. Браузерная очередь и отдельный процесс не обязательны до появления такого адаптера.

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

Точные форматы первой версии находятся в `contracts/schemas/messages`, а ingest-результат - в `contracts/schemas/ingestion/source-result.v1.schema.json`. Поле вида сообщения во всех схемах называется `type`.

Команда сбора самодостаточна: worker получает `job_id`, `attempt_no`, investigation, source, operation, purpose, query, limits и deadline. Ему не требуется доступ к PostgreSQL.

Результат содержит outcome, `record_count`, ссылки на объекты MinIO или manifest, версии adapter/parser/mapping/normalization, digest и предупреждения. Идентификаторы строк `artifacts` назначает Go consumer после проверки результата. Точные поля records и claims определяет ingest-схема.

Есть два взаимоисключающих режима результата:

- inline - `manifest` отсутствует, records и claims находятся в сообщении, а `record_count` равен числу records;
- manifest - ссылка имеет `kind = manifest`, records и claims в сообщении пусты, а `record_count` описывает содержимое manifest.

В обоих режимах сохраняется хотя бы один исходный artifact либо сам manifest. `not_found` имеет `record_count = 0` и не содержит records или claims, но сохраняет исходный ответ источника. Runtime вычисляет `digest` после формирования результата: это SHA-256 канонического JSON тела результата без самого поля `digest`, сериализованного по JSON Canonicalization Scheme. Envelope сообщения в digest не входит.

JSON Schema проверяет переносимую форму сообщения. После неё worker и Go выполняют семантическую проверку: режим manifest не смешивается с inline-данными, `record_count` согласован с records, claim не пуст, временные даты соответствуют precision, а локальные ссылки разрешаются внутри результата. Эти правила остаются кодом доменной границы и не требуют превращать JSON Schema в язык бизнес-логики.

## Жизненный цикл задания

1. Go в одной транзакции создаёт `source_job` со статусом `pending` и outbox message.
2. Publisher блокирует порцию outbox-записей, отправляет сообщения и ждёт publisher confirm.
3. После confirm outbox помечается опубликованным. Job переходит `pending -> queued` только условным обновлением: если consumer уже применил `started` или результат, его состояние не меняется.
4. Worker получает команду и при необходимости сообщает `started`.
5. Worker сохраняет исходные данные и публикует `completed` либо `failed` с классом ошибки.
6. Go consumer сначала регистрирует `message_id` в inbox, затем под блокировкой проверяет job и номер попытки. Актуальный результат разрешено применять из `pending`, потому что его получение подтверждает публикацию команды.
7. Результат применяется один раз, а job получает итоговый статус.

Только Go меняет `source_jobs`. Publisher обновляет статус через `UPDATE ... WHERE status = 'pending'`, поэтому не может вернуть уже начатое или завершённое задание в `queued`. При временной ошибке Go consumer назначает следующую попытку, переводит job в `retry_wait` и при наступлении `next_attempt_at` создаёт новую команду через outbox. Событие `retry_scheduled` при необходимости публикует Go, а не worker.

## Типы сообщений

- `source.collect.requested.v1`;
- `source.collect.started.v1`;
- `source.collect.retry_scheduled.v1`;
- `source.collect.completed.v1`;
- `source.collect.failed.v1`;
- `document.parse.requested.v1`;
- `document.parse.completed.v1`;
- `browser.collect.requested.v1`.

Для самого первого среза `started` и `retry_scheduled` можно не публиковать: достаточно `requested` и одного итогового события. Они добавляются, когда появятся долгие задания и реальные повторы.

## Идемпотентность

Есть три уровня защиты:

1. `consumed_messages` от повторной доставки одного `message_id`;
2. `source_job_results` от двух сообщений с разными ids, но одинаковым результатом job;
3. проверки допустимого перехода статуса и актуального `attempt_no`.

ACK отправляется только после успешной транзакции consumer. При временной технической ошибке consumer отправляет NACK с повторной доставкой; при невалидном или постоянно необрабатываемом сообщении - NACK без возврата в рабочую очередь, после чего RabbitMQ направляет его в DLQ. Publisher считает сообщение отправленным только после подтверждения от RabbitMQ.

## Повторы и ошибки

Ошибки делятся на:

- временные сетевые;
- rate limit;
- постоянные ошибки запроса или формата;
- `action_required` - например CAPTCHA или ручное действие;
- программные ошибки.

Worker передаёт стабильный `code`, безопасный `message`, classification и при необходимости `retry_after_seconds`. Runtime получает classification из общего каталога кодов SDK, а не выбирает её отдельно в каждом адаптере. Go отображает результат так:

| Classification    | Действие Go                                                               |
| ----------------- | ------------------------------------------------------------------------- |
| `transient`       | `retry_wait`, после исчерпания попыток - `unavailable`                    |
| `rate_limited`    | `retry_wait` с учётом `retry_after_seconds`                               |
| `permanent`       | `failed`, кроме отдельно обработанного содержательного результата         |
| `action_required` | `action_required` без автоматического повтора                             |
| `bug`             | `failed`, сообщение уходит в наблюдаемость без внутренних деталей для web |

Ориентировочная задержка повторов: 30 секунд, 2 минуты, 10 минут плюс jitter. Реализация - retry queues с TTL; delayed-message plugin не является обязательной зависимостью.

`not_found` - успешный содержательный результат, а не ошибка. Сообщение в DLQ не должно автоматически менять бизнес-статус: оператор или отдельный reconciler решает, можно ли повторить обработку.

## Отмена

Новое задание для отменённого расследования не публикуется. Worker должен проверять cancellation между длительными шагами. Поздний результат сохраняется как обработанный, но получает `applied = false`; сущности и связи из него автоматически не создаются.
