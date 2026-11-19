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

Ручной код остаётся в `src/shared/api`. Общий `apiClient` задаёт base URL, `X-Request-ID`, JSON transport, `AbortSignal` и безопасное преобразование `application/problem+json` в `ApiProblemError`. Неожиданный error response становится `ApiError` без показа сырого body; network error и отмена запроса остаются различимыми. Клиент не выполняет скрытые retries и не создаёт `Idempotency-Key`: use case передаёт стабильный ключ одной пользовательской команды явно. Для binary response можно передать отдельный parser.

Endpoint-specific функции добавляются вместе с соответствующими пользовательскими сценариями и используют типы из `types.ts`. Отдельный тяжёлый SDK для первого среза не нужен.

Page components не импортируют сгенерированные типы напрямую. Они обращаются к функциям `shared/api`, а transport DTO при необходимости преобразуются в модели интерфейса. JSON Schema сообщений и ingest frontend не использует: RabbitMQ и внутренние данные workers не являются его границей.

`src/shared/api/index.ts` служит публичной границей и реэкспортирует только необходимые transport types, клиент и типы ошибок. Прямой импорт generated-файла или внутренних модулей `shared/api`, а также прямой `fetch` за пределами этого каталога запрещены ESLint. `pnpm check:architecture` выполняет позитивные и негативные пробы этих правил и входит в общий `pnpm check`. TypeScript закреплён на 5.9.3 до появления официальной peer-поддержки TypeScript 6 в `openapi-typescript`.

Base URL задаётся build/dev-server переменной `VITE_API_BASE_URL`; при её отсутствии используется `/api/v1`. Переменная объявлена в корневом `.env.example` и типизирована в `src/env.d.ts`.

## Состояние и базовый интерфейс

Page components не вызывают `fetch` и не сохраняют вторую изменяемую копию server state. Request lifecycle принадлежит use-case composable, добавляемому вместе со сценарием; Pinia хранит UI state либо явно определённый кэш, но расследование, jobs и решения остаются серверным состоянием.

`src/shared/ui` содержит общий layout и нейтральные `LoadingState`, `EmptyState`, `ErrorState`. Компоненты сценариев используют их для единообразных доступных состояний, не подменяя partial result общей ошибкой или пустым экраном.

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
