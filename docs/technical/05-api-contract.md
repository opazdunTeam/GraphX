---
status: draft
owner: tech-lead
reviewers: [backend, frontend]
created: 2026-09-19
updated: 2026-09-19
version: 0.1
---

# Контракт HTTP API

## 1. Назначение

Go API предоставляет единственный публичный программный интерфейс MVP. Точный контракт описывается в `contracts/openapi.yaml` при начале реализации. Этот документ фиксирует семантику, набор ресурсов и общие правила.

Базовый префикс: `/api/v1`. Формат: JSON, UTF-8. Время передаётся в ISO 8601 с часовым поясом, на хранении — UTC. Идентификаторы — UUID в строковом виде.

## 2. Ресурсы MVP

### Расследования

```http
POST   /api/v1/investigations
GET    /api/v1/investigations/{investigation_id}
GET    /api/v1/investigations/{investigation_id}/status
POST   /api/v1/investigations/{investigation_id}/cancel
```

Пример создания:

```json
{
  "subject": {
    "type": "person",
    "name": "Клебанов Александр Яковлевич",
    "birth_date": null,
    "jurisdictions": ["KZ"],
    "identifiers": [],
    "hints": {
      "organization": null,
      "position": null
    }
  },
  "limits": {
    "depth": 1,
    "max_entities": 50
  }
}
```

Ответ — `202 Accepted`:

```json
{
  "id": "uuid",
  "status": "created",
  "created_at": "2026-09-19T12:00:00Z",
  "links": {
    "self": "/api/v1/investigations/uuid",
    "status": "/api/v1/investigations/uuid/status"
  }
}
```

### Кандидаты и решения

```http
GET  /api/v1/investigations/{id}/reviews
GET  /api/v1/investigations/{id}/reviews/{review_id}
POST /api/v1/investigations/{id}/reviews/{review_id}/decision
POST /api/v1/investigations/{id}/decisions/{decision_id}/revert
```

Карточка кандидата содержит признаки за и против, исходные записи, оценку алгоритма и допустимые действия. Клиент не пересчитывает score.

### Результаты

```http
GET /api/v1/investigations/{id}/entities
GET /api/v1/investigations/{id}/entities/{entity_id}
GET /api/v1/investigations/{id}/graph
GET /api/v1/investigations/{id}/timeline
GET /api/v1/investigations/{id}/relationships/{relationship_id}
GET /api/v1/investigations/{id}/evidence/{evidence_id}
```

`graph` возвращает компактную проекцию, а не всю каноническую БД. Узлы и рёбра содержат `result_level`, доступность доказательств и временные поля.

### Расширение поиска

```http
POST /api/v1/investigations/{id}/entities/{entity_id}/expand
```

Команда идемпотентна по заголовку `Idempotency-Key` и возвращает `202 Accepted`.

## 3. Состояние источников

Ответ статуса включает:

```json
{
  "investigation_id": "uuid",
  "status": "running",
  "progress": {
    "total": 3,
    "completed": 1,
    "running": 1,
    "waiting": 1
  },
  "sources": [
    {
      "source": "icij",
      "status": "succeeded",
      "records_found": 4,
      "started_at": "2026-09-19T12:00:01Z",
      "completed_at": "2026-09-19T12:00:04Z",
      "warning": null
    }
  ],
  "requires_review": false,
  "updated_at": "2026-09-19T12:00:05Z"
}
```

Процент выполнения не обещается, если объём работы заранее неизвестен. Интерфейс показывает завершённые этапы и активные источники.

## 4. Ошибки

Используется `application/problem+json`:

```json
{
  "type": "https://docs.example/errors/validation",
  "title": "Некорректные параметры",
  "status": 422,
  "detail": "Не задано имя или идентификатор субъекта",
  "instance": "/api/v1/investigations",
  "code": "INVESTIGATION_SUBJECT_REQUIRED",
  "trace_id": "uuid",
  "fields": [
    {"path": "subject.name", "message": "обязательное поле"}
  ]
}
```

Сообщение предназначено пользователю, `code` — клиентской логике, `trace_id` — диагностике. В ответ не попадают stack trace, секреты и содержимое внутренних исключений.

## 5. Пагинация и фильтры

- Для стабильных списков применяется cursor pagination.
- `limit` имеет серверный максимум.
- Временная шкала поддерживает `from`, `to`, `entity_id`, `event_type`.
- Граф поддерживает `depth`, `result_level` и ограничение числа элементов, но не разрешает клиенту обходить пределы расследования.

## 6. Конкурентные изменения

Решение пользователя содержит ожидаемую версию review. При конфликте API отвечает `409 Conflict`, возвращает актуальное состояние и не перезаписывает более новое решение молча.

## 7. Совместимость

- Новое необязательное поле считается совместимым изменением.
- Удаление или изменение смысла поля требует новой версии API либо периода совместимости.
- Enum расширяются с учётом того, что клиент может получить неизвестное значение.
- OpenAPI и generated client обновляются в одном pull request.
- Каждый публичный endpoint имеет контрактный и интеграционный тест.

## 8. События интерфейса

В первой версии применяется polling. Возможный последующий endpoint:

```http
GET /api/v1/investigations/{id}/events
Accept: text/event-stream
```

SSE сообщает об изменении версии состояния; клиент после события получает актуальную read-модель обычным GET. Это не делает SSE отдельным источником истины.

