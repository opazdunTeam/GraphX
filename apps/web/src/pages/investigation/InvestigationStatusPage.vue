<script setup lang="ts">
import { RouterLink, useRoute, useRouter } from 'vue-router'

const route = useRoute()
const { options } = useRouter()
</script>

<template>
  <section class="investigation-page" aria-labelledby="page-title">
    <h1 id="page-title">Статус расследования</h1>
    <dl class="status-card">
      <div>
        <dt>Идентификатор расследования</dt>
        <dd>{{ route.params.id }}</dd>
      </div>
      <div
        v-if="options.history.state.acceptedInvestigationId === route.params.id"
      >
        <dt>Подтверждение запуска</dt>
        <dd>Принято в работу</dd>
      </div>
    </dl>
    <p>Текущий прогресс на этом экране пока недоступен.</p>
    <RouterLink to="/">Новое расследование</RouterLink>
  </section>
</template>
