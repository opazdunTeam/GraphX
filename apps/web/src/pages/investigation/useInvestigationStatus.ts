import { onScopeDispose, ref, shallowRef, watch, type Ref } from 'vue'

import {
  ApiError,
  getInvestigationStatus,
  type InvestigationStatusSnapshot,
} from '@/shared/api'

const terminalStates = new Set<InvestigationStatusSnapshot['status']>([
  'completed',
  'completed_partial',
  'failed',
  'cancelled',
])

function statusError(cause: unknown) {
  const httpStatus = cause instanceof ApiError ? cause.status : undefined
  const retryable =
    httpStatus === undefined ||
    (httpStatus >= 200 && httpStatus < 300) ||
    httpStatus === 408 ||
    httpStatus === 429 ||
    (httpStatus >= 500 && httpStatus !== 501)
  let description = 'Не удалось получить статус расследования.'
  if (httpStatus === 404) description = 'Расследование не найдено.'
  if (httpStatus === 501) description = 'Просмотр прогресса пока недоступен.'

  return {
    description,
    requestId: cause instanceof ApiError ? cause.requestId : undefined,
    retryable,
  }
}

export function useInvestigationStatus(id: Readonly<Ref<string>>) {
  const snapshot = shallowRef<InvestigationStatusSnapshot>()
  const error = shallowRef<ReturnType<typeof statusError>>()
  const loading = ref(false)
  let retry = () => {}

  watch(
    id,
    (investigationId, _, onCleanup) => {
      snapshot.value = undefined
      error.value = undefined
      let pollTimer: number | undefined
      let requestTimer: number | undefined
      let activeRequest: AbortController | undefined
      let disposed = false
      let stopped = false
      let failures = 0
      let lastRequestFinishedAt = 0
      let jitter = 0

      function schedule() {
        window.clearTimeout(pollTimer)
        if (disposed || activeRequest || stopped) return
        const base = document.hidden
          ? 10_000 + jitter * 5_000
          : 2_000 + jitter * 1_000
        const backoff = 2 ** Math.min(failures, 3)
        const elapsed = performance.now() - lastRequestFinishedAt
        pollTimer = window.setTimeout(
          load,
          Math.max(0, base * backoff - elapsed),
        )
      }

      async function load() {
        if (disposed || activeRequest || stopped) return
        window.clearTimeout(pollTimer)
        const request = new AbortController()
        activeRequest = request
        requestTimer = window.setTimeout(() => request.abort(), 15_000)
        loading.value = true
        try {
          const result = await getInvestigationStatus(
            investigationId,
            request.signal,
          )
          if (disposed) return
          snapshot.value = result
          error.value = undefined
          failures = 0
          stopped = terminalStates.has(result.status)
        } catch (cause) {
          if (disposed) return
          error.value = statusError(cause)
          failures += 1
          stopped = !error.value.retryable
        } finally {
          window.clearTimeout(requestTimer)
          if (!disposed) {
            activeRequest = undefined
            loading.value = false
            lastRequestFinishedAt = performance.now()
            jitter = Math.random()
            schedule()
          }
        }
      }

      retry = () => {
        if (error.value?.retryable) void load()
      }
      document.addEventListener('visibilitychange', schedule)
      onCleanup(() => {
        disposed = true
        window.clearTimeout(pollTimer)
        window.clearTimeout(requestTimer)
        activeRequest?.abort()
        document.removeEventListener('visibilitychange', schedule)
      })
      void load()
    },
    { immediate: true },
  )
  onScopeDispose(() => {
    retry = () => {}
  })

  return { snapshot, error, loading, refresh: () => retry() }
}
