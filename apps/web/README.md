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

Единственным источником типов публичного API служит [`contracts/openapi.yaml`](../../contracts/openapi.yaml). `openapi-typescript` 7.13.0 генерирует типы в:

```text
src/shared/api/generated/openapi.ts
```

Этот файл не редактируется вручную и фиксируется в репозитории. `pnpm generate:api` повторяет генерацию, а `pnpm check:api` сравнивает output с временно сгенерированным файлом и проверяет, что `SubjectInput`, optional limits и dynamic artifact media type не ослаблены. Так изменение обязательного поля или ответа API становится обычной ошибкой TypeScript, а не обнаруживается во время демонстрации.

Ручной код остаётся в `src/shared/api`: здесь задаётся base URL, а функции вызова добавляются вместе с соответствующими пользовательскими сценариями. Они должны передавать `X-Request-ID` и `Idempotency-Key` согласно операции и преобразовывать `application/problem+json` в единый тип ошибки. Отдельный тяжёлый SDK для первого среза не нужен.

Page components не импортируют сгенерированные типы напрямую. Они обращаются к функциям `shared/api`, а transport DTO при необходимости преобразуются в модели интерфейса. JSON Schema сообщений и ingest frontend не использует: RabbitMQ и внутренние данные workers не являются его границей.

`src/shared/api/index.ts` служит публичной границей и реэкспортирует только необходимые transport types и настройку base URL. TypeScript закреплён на 5.9.3 до появления официальной peer-поддержки TypeScript 6 в `openapi-typescript`.

Base URL задаётся build/dev-server переменной `VITE_API_BASE_URL`; при её отсутствии используется `/api/v1`. Переменная объявлена в корневом `.env.example` и типизирована в `src/env.d.ts`.

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
pnpm generate:api
pnpm check:api
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
pnpm generate:api
pnpm check:api
pnpm check
pnpm build
```

Dev server работает на `http://localhost:5173` и проксирует `/api` на `http://localhost:8080`.

См. [описание интерфейса](../../docs/13-frontend.md).
