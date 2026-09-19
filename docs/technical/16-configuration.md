---
status: draft
owner: tech-lead
reviewers: [project-team]
created: 2026-09-19
updated: 2026-09-19
version: 0.1
---

# Конфигурация окружения

## 1. Принципы

- Переменные окружения задают различия окружений; предметные данные хранятся в БД.
- `.env.example` документирует имена и безопасные значения, но не содержит секретов.
- `.env` и `.env.locale` не коммитятся.
- Каждый процесс при запуске валидирует обязательные переменные и прекращает работу с понятной ошибкой при некорректном значении.
- Секреты не передаются через RabbitMQ и не возвращаются диагностическими endpoint.
- Имена имеют префикс области и единый формат `UPPER_SNAKE_CASE`.

## 2. Группы переменных

### Общее

```dotenv
APP_ENV=local
LOG_LEVEL=info
LOG_FORMAT=json
```

### Go API

```dotenv
API_HTTP_ADDR=:8080
API_PUBLIC_BASE_URL=http://localhost:8080
API_SHUTDOWN_TIMEOUT=15s
```

### PostgreSQL

```dotenv
POSTGRES_DSN=postgres://graphx:change-me@postgres:5432/graphx?sslmode=disable
POSTGRES_MAX_CONNS=20
POSTGRES_MIN_CONNS=2
```

### RabbitMQ

```dotenv
RABBITMQ_URL=amqp://graphx:change-me@rabbitmq:5672/graphx
RABBITMQ_PREFETCH_SOURCE=4
RABBITMQ_PREFETCH_DOCUMENT=2
RABBITMQ_PREFETCH_BROWSER=1
```

### MinIO

```dotenv
S3_ENDPOINT=http://minio:9000
S3_REGION=local
S3_BUCKET_ARTIFACTS=graphx-artifacts
S3_ACCESS_KEY=change-me
S3_SECRET_KEY=change-me
S3_USE_PATH_STYLE=true
```

### Ограничения расследования

```dotenv
INVESTIGATION_DEFAULT_DEPTH=1
INVESTIGATION_MAX_DEPTH=2
INVESTIGATION_DEFAULT_ENTITY_LIMIT=50
INVESTIGATION_MAX_ENTITY_LIMIT=100
INVESTIGATION_SOURCE_TIMEOUT=2m
```

API принимает пользовательское значение только в пределах hard limit из конфигурации. Изменение лимита влияет на стоимость, время и размер графа, поэтому production/demo значения проходят ревью.

### Воркеры

```dotenv
WORKER_KIND=source
WORKER_CONCURRENCY=4
WORKER_HEARTBEAT_INTERVAL=15s
WORKER_JOB_TIMEOUT=5m
PLAYWRIGHT_ENABLED=false
```

### Источники

```dotenv
SOURCE_ICIJ_ENABLED=true
SOURCE_OPENSANCTIONS_ENABLED=true
SOURCE_OPENCORPORATES_ENABLED=false
SOURCE_OPENCORPORATES_API_KEY=
```

Секреты конкретного источника читаются только worker-процессом, которому они нужны. Политика rate limit имеет безопасное значение в metadata адаптера и при необходимости ограничивается окружением ещё сильнее.

## 3. Что не помещается в `.env`

- каталог источников и их смысл;
- правила entity resolution и веса алгоритма после появления администрируемой версии;
- пользовательские запросы;
- большие JSON-конфигурации;
- schema сообщений;
- секреты в реальном значении внутри `.env.example`.

Для MVP стабильные технические defaults могут находиться в коде, а переменная окружения только переопределяет их. Значение, которое должно изменяться пользователем без перезапуска, хранится в БД или становится настройкой расследования.

## 4. Приоритет и загрузка

Рекомендуемый порядок:

1. безопасный default в коде;
2. переменная окружения;
3. допустимый параметр конкретного расследования, ограниченный server-side maximum.

Фактическая конфигурация логируется только как перечень режимов и несекретных значений. Пароли, DSN, API keys и cookies маскируются полностью.

## 5. Изменение конфигурации

- Добавление обязательной переменной сопровождается обновлением `.env.example`, Compose и deployment documentation.
- Удаление переменной проходит период совместимости хотя бы одного релиза.
- Изменение лимита не должно молча менять уже выполняемое расследование: фактические пределы фиксируются в записи расследования при создании.
