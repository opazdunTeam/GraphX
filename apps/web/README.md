# GraphX Web

Стартовое Vue 3 + Vite + TypeScript SPA. Клиент использует только публичный HTTP API и не обращается напрямую к PostgreSQL, RabbitMQ или MinIO.

Первый интерфейс включает:

- запуск расследования;
- polling прогресса и частичных результатов;
- выбор неоднозначного кандидата;
- карточки сущностей и доказательств;
- компактный граф и временную шкалу.

Transport DTO преобразуются в `shared/api`/entity models, а не разбираются внутри page components.

```powershell
npm ci
npm run dev
npm run check
npm run build
```

Dev server работает на `http://localhost:5173` и проксирует `/api` на `http://localhost:8080`.

См. [архитектуру интерфейса](../../docs/technical/10-frontend-architecture.md).
