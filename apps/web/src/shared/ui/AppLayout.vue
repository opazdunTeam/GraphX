<script setup lang="ts">
import { RouterLink } from 'vue-router'
</script>

<template>
  <div class="app-layout">
    <header class="app-header">
      <RouterLink class="app-brand" to="/">GraphX</RouterLink>
      <nav
        v-if="$slots.navigation"
        class="app-navigation"
        aria-label="Основная навигация"
      >
        <slot name="navigation" />
      </nav>
    </header>
    <main class="app-content">
      <slot />
    </main>
  </div>
</template>
