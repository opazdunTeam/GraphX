<script setup lang="ts">
import { RouterLink, useRouter } from 'vue-router'

import { EmptyState, ErrorState, LoadingState } from '@/shared/ui'

import { useInvestigationForm } from './useInvestigationForm'

const router = useRouter()
const {
  type,
  name,
  identifierType,
  identifier,
  selectedCodes,
  sources,
  catalogLoading,
  catalogError,
  submitting,
  formError,
  submitError,
  accepted,
  loadSources,
  submit,
} = useInvestigationForm()

async function startInvestigation() {
  const result = await submit()
  if (result) {
    await router.push({
      name: 'investigation-status',
      params: { id: result.id },
      state: { acceptedInvestigationId: result.id },
    })
  }
}
</script>

<template>
  <section class="investigation-page" aria-labelledby="page-title">
    <h1 id="page-title">Новое расследование</h1>
    <form @submit.prevent="startInvestigation">
      <fieldset :disabled="submitting || !!accepted">
        <legend>Субъект</legend>
        <label>
          Тип субъекта
          <select v-model="type" @change="selectedCodes = []">
            <option value="person">Человек</option>
            <option value="organization">Организация</option>
          </select>
        </label>
        <label>
          {{
            type === 'organization' ? 'Название организации' : 'Имя человека'
          }}
          <input v-model="name" name="subject-name" />
        </label>
        <p id="identifier-hint">
          Можно указать известный идентификатор вместо имени или названия.
        </p>
        <div class="identifier-fields">
          <label>
            Тип идентификатора
            <input
              v-model="identifierType"
              name="identifier-type"
              aria-describedby="identifier-hint"
            />
          </label>
          <label>
            Значение идентификатора
            <input
              v-model="identifier"
              name="identifier-value"
              aria-describedby="identifier-hint"
            />
          </label>
        </div>
      </fieldset>
      <fieldset :disabled="submitting || !!accepted">
        <legend>Источники</legend>
        <LoadingState v-if="catalogLoading" label="Загружаем источники…" />
        <ErrorState
          v-else-if="catalogError"
          title="Не удалось загрузить источники"
          description="Повторите загрузку списка, чтобы выбрать источники для расследования."
          :request-id="catalogError.requestId"
          retryable
          @retry="loadSources"
        />
        <EmptyState
          v-else-if="!sources.length"
          title="Нет доступных источников"
          description="Для этого типа субъекта сейчас нет включённых источников поиска."
        >
          <template #actions>
            <button type="button" @click="loadSources">Обновить список</button>
          </template>
        </EmptyState>
        <label
          v-for="source in sources"
          v-else
          :key="source.code"
          class="source-option"
        >
          <input v-model="selectedCodes" type="checkbox" :value="source.code" />
          {{ source.name }}
        </label>
      </fieldset>
      <p v-if="formError" class="validation-error" role="alert">
        {{ formError }}
      </p>
      <ErrorState
        v-if="submitError"
        :title="submitError.title"
        :description="submitError.description"
        :request-id="submitError.requestId"
        :retryable="submitError.refreshSources"
        retry-label="Обновить источники"
        @retry="loadSources"
      />
      <RouterLink
        v-if="accepted"
        :to="{
          name: 'investigation-status',
          params: { id: accepted.id },
          state: { acceptedInvestigationId: accepted.id },
        }"
      >
        Открыть статус расследования
      </RouterLink>
      <button
        v-else
        class="primary-action"
        type="submit"
        :disabled="
          catalogLoading || !!catalogError || !sources.length || submitting
        "
      >
        {{ submitting ? 'Отправляем…' : 'Запустить расследование' }}
      </button>
    </form>
  </section>
</template>
