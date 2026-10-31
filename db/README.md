# База данных GraphX

`migrations/` содержит вручную написанные SQL-миграции goose. Схема не управляется ORM/AutoMigrate.

План последовательности миграций:
1. расширения PostgreSQL, `investigations`, `sources`;
2. `source_jobs`, outbox, inbox, business deduplication и API idempotency;
3. `artifacts`, `job_artifacts`, `processing_runs`, `source_records`, `claims`;
4. `entities`, `entity_names`, `entity_identifiers`, `entity_records`;
5. reviews, candidates и decisions;
6. relationships, supporting claims и состав расследования;
7. events - только после проверки реальных event claims.

Смысл таблиц и основные инварианты описаны в [модели данных](../docs/07-data-model.md). После появления миграций именно SQL определяет точную физическую схему. До первой миграции каталог сохраняется существующим `.gitkeep`.
