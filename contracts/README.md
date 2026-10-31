# Контракты GraphX

Каталог содержит машиночитаемые интерфейсы между компонентами:
- `openapi.yaml` - HTTP API Go ↔ Vue;
- `schemas/messages/message-envelope.v1.schema.json` - общий конверт сообщений RabbitMQ;
- `schemas/messages/source-collect-requested.v1.schema.json` - команда сбора источника;
- `schemas/messages/source-collect-completed.v1.schema.json` - успешный результат сбора;
- `schemas/messages/source-collect-failed.v1.schema.json` - ошибка сбора;
- `schemas/ingestion/source-result.v1.schema.json` - records, claims и ссылки на artifacts;
- `asyncapi.yaml` добавляется только после стабилизации messaging schemas.

Поле вида сообщения во всех транспортах называется `type`. Новая несовместимая версия получает новый суффикс в `type` и отдельный schema-файл.
