# ADR-0012. Попытки задания и бизнес-идемпотентность сообщений

- **Статус:** accepted
- **Дата:** 2026-10-02
- **Владельцы:** tech-lead, backend, data

## Контекст

RabbitMQ доставляет сообщения как минимум один раз. Повтор одной команды может породить новое result message с новым `message_id`, поэтому inbox по ID сообщения не предотвращает повторное применение одного бизнес-результата. События `started`, `retry_scheduled` и `completed` также могут прибыть повторно или с задержкой.

## Решение

`job_id` идентифицирует бизнес-работу, `message_id` — одну публикацию, `attempt_no` и `attempt_id` — конкретную попытку. Worker events дополнительно содержат `root_command_id`; terminal result содержит `result_digest`.

Go consumer сначала дедуплицирует публикацию через inbox, затем применяет guarded business transition по `job_id + attempt_no`. Терминальное состояние не понижается поздним `started` или `retry_scheduled`. Ingestion одного результата защищается устойчивым business key, включающим job, artifact/result digest и processing version.

Worker подтверждает команду только после publisher confirm исходящего результата или retry-команды. Contract-invalid команды и события направляются в отдельные DLQ. Исчерпание попыток всегда порождает терминальный `failed` event; DLQ не считается заменой бизнес-события.

## Рассмотренные варианты

- дедупликация только по `message_id`;
- exactly-once transport;
- доступ Python workers к PostgreSQL inbox;
- отказ от событий `started` и `retry_scheduled`.

## Последствия

- JSON Schema всех worker events содержит attempt fields;
- нужны тесты crash-after-publish-before-ack, duplicate result, out-of-order started/retry и late result after cancellation;
- одна входная очередь обрабатывается единым Go event router, который валидирует тип и вызывает нужный application handler;
- транспортное состояние RabbitMQ не заменяет business state `source_jobs`.

## Условия пересмотра

Замена брокера или перенос выполнения заданий в единый процесс с общей транзакционной БД.
