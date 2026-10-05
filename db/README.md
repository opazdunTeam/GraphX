# База данных GraphX

`migrations/` содержит вручную написанные SQL-миграции goose. Схема не создаётся при старте API и не управляется ORM/AutoMigrate.

План стартовой последовательности:

1. extensions;
2. sources;
3. investigations, jobs и messaging reliability;
4. provenance и processing runs;
5. entities и factual relationships;
6. entity matching;
7. temporal events - только после проверки реальных event claims.

Точные таблицы и инварианты описаны в [модели данных](../docs/technical/04-data-model.md). До первой SQL-миграции каталог сохраняется существующим `.gitkeep`.
