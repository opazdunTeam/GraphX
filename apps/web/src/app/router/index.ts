import { createRouter, createWebHistory } from 'vue-router'

import HomePage from '@/pages/home/HomePage.vue'
import InvestigationStatusPage from '@/pages/investigation/InvestigationStatusPage.vue'

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      name: 'home',
      component: HomePage,
    },
    {
      path: '/investigations/:id/status',
      name: 'investigation-status',
      component: InvestigationStatusPage,
    },
  ],
})
