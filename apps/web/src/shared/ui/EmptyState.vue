<script setup lang="ts">
defineProps<{
  description?: string
  title: string
}>()
</script>

<template>
  <section class="async-state" role="status">
    <h2>{{ title }}</h2>
    <p v-if="description">{{ description }}</p>
    <div v-if="$slots.actions" class="state-actions">
      <slot name="actions" />
    </div>
  </section>
</template>
