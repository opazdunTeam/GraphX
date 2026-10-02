---
status: draft
owner: tech-lead
reviewers: [backend, frontend]
created: 2026-09-19
updated: 2026-10-02
version: 0.3
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
  "status": "running",
  "stage": "subject_discovery",
  "state_version": 1,
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

Создание расследования запускает только поиск исходного контрагента. Если кандидат удовлетворяет сильному правилу без конфликтов, сервер фиксирует review и автоматическое решение, а затем сам планирует enrichment. Сам факт наличия ровно одного результата недостаточен для автовыбора. Нулевая выдача завершается как корректный результат «кандидаты не найдены», а не как системная ошибка.

При неоднозначности status endpoint возвращает `requires_action = true` и ссылку на `review`. Клиент получает актуальный набор кандидатов и отправляет одно решение:

```json
{
  "id": "uuid",
  "type": "initial_subject",
  "status": "pending",
  "version": 3,
  "candidates": [
    {
      "id": "uuid",
      "display_name": "Клебанов Александр Яковлевич",
      "summary": {
        "jurisdictions": ["KZ"],
        "organizations": ["пример организации"]
      },
      "score": 0.73,
      "supporting_features": ["совпало полное имя"],
      "conflicts": ["дата рождения не найдена"]
    }
  ]
}
```

Затем клиент отправляет:

```json
{
  "expected_version": 3,
  "decision": "select_candidate",
  "candidate_id": "uuid",
  "reason": null
}
```

`expected_version` соответствует `match_reviews.lock_version`. Команда передаётся с `Idempotency-Key`. Сервер блокирует review, проверяет его версию и принадлежность кандидата этому набору, сохраняет решение и автоматически планирует полный сбор. Успешный ответ — `202 Accepted` с обновлёнными `status`, `stage` и `state_version`. Отдельного запроса «продолжить расследование» нет.

Для initial-subject review допустимо решение `none_of_the_above`: review закрывается без создания корневой entity, enrichment не запускается, а расследование получает корректный результат «субъект не разрешён». Уточнение исходных данных в MVP создаёт новое расследование, чтобы не смешивать два пользовательских запроса и набора кандидатов.

### Результаты

```http
GET /api/v1/investigations/{id}/entities
GET /api/v1/investigations/{id}/entities/{entity_id}
GET /api/v1/investigations/{id}/graph
GET /api/v1/investigations/{id}/timeline
GET /api/v1/investigations/{id}/relationships/{relationship_id}
GET /api/v1/investigations/{id}/claims/{claim_id}/evidence
```

`graph` возвращает компактную проекцию, а не всю каноническую БД. Узлы и рёбра содержат `result_level`, доступность доказательств и временные поля. `fact` читается из актуальных канонических relationships; `derived` и `hypothesis` вычисляются в контексте расследования и не обязаны иметь отдельную строку в БД.

Evidence не является отдельной предметной сущностью MVP. `claim_id` разрешается в claim, source record, artifact metadata и точный locator/excerpt. API выдаёт содержимое artifact только через контролируемый endpoint и не раскрывает внутренний MinIO URL.

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
  "stage": "subject_discovery",
  "state_version": 4,
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
  "requires_action": false,
  "action": null,
  "updated_at": "2026-09-19T12:00:05Z"
}
```

Процент выполнения не обещается, если объём работы заранее неизвестен. Интерфейс показывает завершённые этапы и активные источники.

Если требуется выбор пользователя, фрагмент ответа выглядит так:

```json
{
  "status": "running",
  "stage": "subject_resolution",
  "state_version": 7,
  "requires_action": true,
  "action": {
    "type": "resolve_candidate",
    "review_id": "uuid",
    "href": "/api/v1/investigations/uuid/reviews/uuid"
  }
}
```

Status endpoint читает компактное сохранённое состояние расследования. Он не обращается к RabbitMQ, Python-воркерам и внешним источникам и не пересчитывает граф.

API-поле `requires_action` вычисляется из незакрытых reviews (`pending_review_count > 0`); отдельный конкурирующий boolean в БД не хранится.

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

Решение пользователя содержит `expected_version` review. При конфликте, уже закрытом либо заменённом наборе API отвечает `409 Conflict`, возвращает ссылку на актуальное состояние и не перезаписывает более новое решение молча. Повтор команды с тем же `Idempotency-Key` возвращает сохранённый результат первой обработки. Повтор того же ключа с другим hash тела возвращает `409 Conflict`; ключ устойчиво хранится в `api_idempotency_keys`, а не только в памяти API.

## 7. Совместимость

- Новое необязательное поле считается совместимым изменением.
- Удаление или изменение смысла поля требует новой версии API либо периода совместимости.
- Enum расширяются с учётом того, что клиент может получить неизвестное значение.
- OpenAPI и generated client обновляются в одном pull request.
- Каждый публичный endpoint имеет контрактный и интеграционный тест.

## 8. Polling, ETag и события интерфейса

В MVP применяется polling status endpoint раз в 2–3 секунды. Клиент:

- добавляет небольшой случайный jitter к интервалу;
- не запускает параллельные циклы для одного расследования;
- увеличивает интервал после сетевых ошибок;
- замедляет его до 10–15 секунд в скрытой вкладке;
- прекращает polling при `completed`, `completed_partial`, `failed` или `cancelled`.

Поле `state_version` закладывается в MVP. После MVP status endpoint может вернуть:

```http
ETag: W/"investigation-{id}-{state_version}"
```

Клиент сможет передавать `If-None-Match`; при неизменившейся версии API ответит `304 Not Modified` без тела. ETag оптимизирует передачу и сериализацию, но не отменяет сам HTTP-запрос.

Предусмотренный последующий SSE endpoint:

```http
GET /api/v1/investigations/{id}/events
Accept: text/event-stream
```

SSE сообщает только `investigation_id`, новую `state_version` и тип изменения. Клиент после события получает актуальную read-модель обычным GET, при необходимости с ETag. После разрыва соединения он повторно запрашивает status, поэтому SSE не является отдельным источником истины.

SSE вводится после MVP при подтверждённой потребности. WebSocket для редких односторонних обновлений и webhook для браузера не используются. Первое развёртывание рассчитано на один экземпляр API; горизонтальное масштабирование не является требованием MVP.
