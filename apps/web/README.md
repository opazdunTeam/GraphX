# GraphX Web

Стартовое Vue 3 + Vite + TypeScript SPA. Клиент использует только публичный HTTP API и не обращается напрямую к PostgreSQL, RabbitMQ или MinIO.

Первый интерфейс включает:
- запуск расследования;
- polling прогресса и частичных результатов;
- выбор неоднозначного кандидата;
- карточки сущностей и доказательств;
- компактный граф и временную шкалу.

Transport DTO преобразуются в `shared/api`/entity models, а не разбираются внутри page components.

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
