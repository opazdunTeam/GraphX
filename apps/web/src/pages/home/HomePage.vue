<script setup lang="ts">
import { apiBaseUrl } from '@/shared/api/client'
</script>

<template>
  <main class="shell">
    <section class="hero" aria-labelledby="page-title">
      <p class="eyebrow">Investigation workspace</p>
      <h1 id="page-title">GraphX</h1>
      <p class="lead">
        Каркас интерфейса готов. Следующий вертикальный срез добавит запуск
        расследования, прогресс источников и проверяемые доказательства.
      </p>
      <dl class="status-card">
        <div>
          <dt>Web</dt>
          <dd>ready</dd>
        </div>
        <div>
          <dt>API base</dt>
          <dd>{{ apiBaseUrl }}</dd>
        </div>
      </dl>
    </section>
  </main>
</template>
