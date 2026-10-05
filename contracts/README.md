# Контракты GraphX

Каталог содержит машиночитаемые интерфейсы между компонентами:

- `openapi.yaml` - HTTP API Go ↔ Vue;
- `schemas/messages/` - JSON Schema RabbitMQ commands/events после их фиксации;
- `schemas/ingestion/` - JSON Schema результатов adapters;
- `asyncapi.yaml` добавляется только после стабилизации messaging schemas.

Изменение контракта выполняется вместе с consumer/producer tests. Markdown объясняет смысл, но реализованный точный формат задаёт соответствующий машиночитаемый файл.

Пустые schema-каталоги заранее не создаются: первый файл появляется вместе с первым утверждённым контрактом.
