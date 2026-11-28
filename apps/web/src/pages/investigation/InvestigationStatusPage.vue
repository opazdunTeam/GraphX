<script setup lang="ts">
import { computed } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'

import type { InvestigationStatusSnapshot } from '@/shared/api'
import { EmptyState, ErrorState, LoadingState } from '@/shared/ui'

import { useInvestigationStatus } from './useInvestigationStatus'

const route = useRoute()
const { options } = useRouter()
const investigationId = computed(() => String(route.params.id))
const { snapshot, error, loading, refresh } =
  useInvestigationStatus(investigationId)

const investigationLabels: Record<
  InvestigationStatusSnapshot['status'],
  string
> = {
  created: 'Создано',
  running: 'В работе',
  completed: 'Завершено',
  completed_partial: 'Завершено частично',
  failed: 'Завершено с ошибкой',
  cancelled: 'Отменено',
}

const stageLabels: Record<InvestigationStatusSnapshot['stage'], string> = {
  subject_discovery: 'Поиск субъекта',
  subject_resolution: 'Уточнение совпадений',
  enrichment: 'Сбор дополнительных данных',
  analysis: 'Анализ',
  completed: 'Завершение',
}

const sourceLabels: Record<
  InvestigationStatusSnapshot['sources'][number]['status'],
  string
> = {
  pending: 'Ожидает запуска',
  running: 'В работе',
  retry_wait: 'Ожидает повторной попытки',
  action_required: 'Требуется действие',
  succeeded: 'Результат получен',
  not_found: 'Ничего не найдено',
  completed_partial: 'Часть результатов получена',
  unavailable: 'Недоступен',
  failed: 'Ошибка',
  cancelled: 'Отменён',
}

const dateTimeFormat = new Intl.DateTimeFormat('ru-RU', {
  dateStyle: 'short',
  timeStyle: 'medium',
})

function formatDateTime(value: string) {
  const timestamp = Date.parse(value)
  return Number.isNaN(timestamp) ? value : dateTimeFormat.format(timestamp)
}
</script>

<template>
  <section class="investigation-page" aria-labelledby="page-title">
    <h1 id="page-title">Статус расследования</h1>
    <dl class="status-card">
      <div>
        <dt>Идентификатор расследования</dt>
        <dd>{{ investigationId }}</dd>
      </div>
      <div
        v-if="options.history.state.acceptedInvestigationId === investigationId"
      >
        <dt>Подтверждение запуска</dt>
        <dd>Принято в работу</dd>
      </div>
    </dl>
    <LoadingState
      v-if="loading && !snapshot && !error"
      label="Загружаем статус расследования…"
    />
    <ErrorState
      v-if="error"
      :title="snapshot ? 'Не удалось обновить прогресс' : 'Статус недоступен'"
      :description="error.description"
      :request-id="error.requestId"
      :retryable="error.retryable && !loading"
      @retry="refresh"
    />
    <template v-if="snapshot">
      <dl class="status-card">
        <div>
          <dt>Расследование</dt>
          <dd>{{ investigationLabels[snapshot.status] }}</dd>
        </div>
        <div>
          <dt>Этап</dt>
          <dd>{{ stageLabels[snapshot.stage] }}</dd>
        </div>
        <div>
          <dt>Завершено заданий</dt>
          <dd>
            {{ snapshot.progress.terminal_jobs }} из
            {{ snapshot.progress.known_jobs }} известных
          </dd>
        </div>
      </dl>

      <section
        v-if="snapshot.partial_result.succeeded_jobs > 0"
        aria-labelledby="result-summary-title"
      >
        <h2 id="result-summary-title">Полученные результаты</h2>
        <dl class="status-card">
          <div>
            <dt>Заданий с результатом</dt>
            <dd>{{ snapshot.partial_result.succeeded_jobs }}</dd>
          </div>
          <div>
            <dt>Источников с результатом</dt>
            <dd>{{ snapshot.partial_result.sources_with_results }}</dd>
          </div>
        </dl>
      </section>

      <section aria-labelledby="source-status-title">
        <h2 id="source-status-title">Источники расследования</h2>
        <EmptyState
          v-if="snapshot.sources.length === 0"
          title="Источников пока нет"
        />
        <ul v-else class="source-status-list">
          <li v-for="source in snapshot.sources" :key="source.source_code">
            <h3>{{ source.source_code }}</h3>
            <dl class="status-card">
              <div>
                <dt>Состояние</dt>
                <dd>{{ sourceLabels[source.status] }}</dd>
              </div>
              <div>
                <dt>Попытки</dt>
                <dd>{{ source.attempt_count }}</dd>
              </div>
              <div v-if="source.succeeded_job_count > 0">
                <dt>Заданий с результатом</dt>
                <dd>{{ source.succeeded_job_count }}</dd>
              </div>
              <div>
                <dt>Последнее изменение</dt>
                <dd>
                  <time :datetime="source.updated_at">
                    {{ formatDateTime(source.updated_at) }}
                  </time>
                </dd>
              </div>
              <div v-if="source.next_attempt_at">
                <dt>Следующая попытка</dt>
                <dd>
                  <time :datetime="source.next_attempt_at">
                    {{ formatDateTime(source.next_attempt_at) }}
                  </time>
                </dd>
              </div>
              <div v-if="source.error_detail || source.error_code">
                <dt>Последняя проблема</dt>
                <dd>{{ source.error_detail || source.error_code }}</dd>
              </div>
            </dl>
          </li>
        </ul>
      </section>
    </template>
    <RouterLink to="/">Новое расследование</RouterLink>
  </section>
</template>

<style scoped>
.investigation-page > :not(h1) {
  margin-block: 1rem;
}

.source-status-list {
  display: grid;
  gap: 1rem;
  padding: 0;
  list-style: none;
}

.source-status-list h3 {
  margin: 0 0 0.5rem;
  overflow-wrap: anywhere;
}
</style>
