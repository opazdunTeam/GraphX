# База данных GraphX

`migrations/` содержит вручную написанные SQL-миграции goose. Схема не управляется ORM/AutoMigrate.

Текущая последовательность миграций:

1. `001_core.sql` - shared extension `pg_trgm`, `investigations` и `sources`;
2. `002_messaging_jobs.sql` - outbox, inbox, `source_jobs`, business deduplication и API idempotency;
3. `003_ingestion.sql` - `artifacts`, provenance результата, `processing_runs`, `source_records` и `claims`;
4. `004_seed_sources.sql` - идемпотентный минимальный каталог sources с fixture; эта версия завершает стартовую серию задачи #8. Down намеренно является no-op: справочная строка может быть связана с provenance-данными и окончательно удаляется вместе с `sources` при откате `001_core`;
5. `005_entities.sql` - `entities`, активные проекции имён и идентификаторов;
6. `006_matching.sql` - reviews, candidates, append-only decisions, обратимые links записей к entities и состав расследования; эта версия завершает задачу #72;
7. `007_artifact_lineage.sql` - безопасное исходное имя и parent provenance неизменяемых artifacts;
8. `008_outbox_leases.sql` - временная аренда outbox-сообщений для безопасной работы нескольких publishers;
9. `009_investigation_creation.sql` - разделение raw и normalized subject и начальная версия состояния investigation.
10. `010_outbox_payload_representation.sql` - защита текстового представления payload для стабильной повторной публикации.

Смысл таблиц и основные инварианты описаны в [модели данных](../docs/07-data-model.md). SQL определяет точную физическую схему, а ERD должна обновляться вместе с изменением состава таблиц и колонок.

Миграции запускаются отдельно от API. Команды `migrate-status`, `migrate-up`, `migrate-down` и защищённый `migrate-reset-local` определены в [`apps/api/Makefile`](../apps/api/Makefile). Они используют `POSTGRES_DSN` и закреплённую версию goose. Локальный reset допустим только для пустой учебной базы с `APP_ENV=local` и `CONFIRM_RESET=1`.

`make test-integration` поднимает PostgreSQL 18, проверяет оба пути владения `pg_trgm` (preinstalled и созданный миграцией), защиту ownership-marker, конфликтующий и повторный fixture seed, no-op rollback fixture после появления зависимого job, границы версий 4/5/6/7/8/9, отказ перехода 5→6 при любых active/inactive orphan projections и конкурентную блокировку их вставки, аренду и повторную выдачу outbox-сообщений, создание investigation и атомарность его начальных jobs, pgx и migration integration tests, полный goose reset. После прогона удаляются тестовые контейнер и сеть; данные PostgreSQL хранятся в `tmpfs` и исчезают вместе с контейнером.
