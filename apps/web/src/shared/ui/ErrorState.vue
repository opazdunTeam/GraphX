<script setup lang="ts">
withDefaults(
  defineProps<{
    description: string
    requestId?: string
    retryable?: boolean
    retryLabel?: string
    title?: string
  }>(),
  {
    requestId: undefined,
    retryable: false,
    retryLabel: 'Повторить',
    title: 'Не удалось загрузить данные',
  },
)

defineEmits<{
  retry: []
}>()
</script>

<template>
  <section class="async-state error-state" role="alert" aria-live="assertive">
    <h2>{{ title }}</h2>
    <p>{{ description }}</p>
    <p v-if="requestId" class="request-id">
      Идентификатор запроса: {{ requestId }}
    </p>
    <button v-if="retryable" type="button" @click="$emit('retry')">
      {{ retryLabel }}
    </button>
  </section>
</template>
