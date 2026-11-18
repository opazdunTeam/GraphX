<script setup lang="ts">
withDefaults(
  defineProps<{
    label?: string
  }>(),
  {
    label: 'Загрузка…',
  },
)
</script>

<template>
  <section
    class="async-state"
    role="status"
    aria-live="polite"
    aria-busy="true"
  >
    <span class="loading-indicator" aria-hidden="true" />
    <p>{{ label }}</p>
  </section>
</template>
