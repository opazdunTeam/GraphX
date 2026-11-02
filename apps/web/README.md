# GraphX Web

Стартовое Vue 3 + Vite + TypeScript SPA. Клиент использует только публичный HTTP API и не обращается напрямую к PostgreSQL, RabbitMQ или MinIO.

Первый интерфейс включает:

- запуск расследования;
- polling прогресса и частичных результатов;
- выбор неоднозначного кандидата;
- карточки сущностей и доказательств;
- компактный граф и временную шкалу.

Transport DTO преобразуются в `shared/api`/entity models, а не разбираются внутри page components.

## Использование OpenAPI

Единственным источником типов публичного API служит [`contracts/openapi.yaml`](../../contracts/openapi.yaml). При реализации первого пользовательского сценария `openapi-typescript` должен генерировать типы в:

```text
src/shared/api/generated/openapi.ts
```

Этот файл не редактируется вручную и фиксируется в репозитории. В `package.json` добавляется отдельная команда `generate:api`, а CI повторяет генерацию и проверяет отсутствие diff. Так изменение обязательного поля или ответа API становится обычной ошибкой TypeScript, а не обнаруживается во время демонстрации.

Ручной код остаётся в `src/shared/api`: он задаёт base URL, `X-Request-ID`, `Idempotency-Key`, обработку `application/problem+json` и удобные функции вызова. Для первого среза достаточно сгенерированных типов и небольшого клиента; отдельный тяжёлый SDK не нужен. Если число операций вырастет, можно подключить типизированный клиент поверх тех же OpenAPI-типов.

Page components не импортируют сгенерированные типы напрямую. Они обращаются к функциям `shared/api`, а transport DTO при необходимости преобразуются в модели интерфейса. JSON Schema сообщений и ingest frontend не использует: RabbitMQ и внутренние данные workers не являются его границей.

Генерация пока не подключена, потому что базовые endpoints ещё не реализованы. Она добавляется одновременно с первым реальным вызовом API.

## Запуск и проверка

Все команды ниже выполняются из корня репозитория.

### Windows (PowerShell)

```powershell
cd apps/web
pnpm install --frozen-lockfile
pnpm dev
```

```powershell
# Проверки
cd apps/web
pnpm check
pnpm build
```

### Linux

```bash
cd apps/web
pnpm install --frozen-lockfile
pnpm dev
```

```bash
# Проверки
cd apps/web
pnpm check
pnpm build
```

Dev server работает на `http://localhost:5173` и проксирует `/api` на `http://localhost:8080`.

См. [описание интерфейса](../../docs/13-frontend.md).
