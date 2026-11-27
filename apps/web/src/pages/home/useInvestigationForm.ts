import { computed, onMounted, onScopeDispose, ref, shallowRef } from 'vue'

import {
  ApiError,
  ApiProblemError,
  createInvestigation,
  createUuid,
  listSources,
  type CreateInvestigationRequest,
  type InvestigationAccepted,
  type SourceCatalog,
  type SubjectInput,
} from '@/shared/api'

const problemMessages: Record<string, { title: string; description: string }> =
  {
    INVALID_REQUEST: {
      title: 'Проверьте данные расследования',
      description:
        'Сервер отклонил введённые данные. Проверьте сведения о субъекте.',
    },
    SOURCE_NOT_AVAILABLE: {
      title: 'Источник недоступен для запуска',
      description: 'Обновите список источников и проверьте выбор.',
    },
    IDEMPOTENCY_IN_PROGRESS: {
      title: 'Запуск уже обрабатывается',
      description: 'Подождите немного и повторите отправку с теми же данными.',
    },
    IDEMPOTENCY_CONFLICT: {
      title: 'Конфликт при повторной отправке',
      description: 'Сервер уже связал эту команду с другим запросом.',
    },
    REQUEST_TOO_LARGE: {
      title: 'Слишком много данных',
      description: 'Сократите введённые сведения и отправьте форму снова.',
    },
  }

function submissionError(error: unknown) {
  const message =
    error instanceof ApiProblemError &&
    Object.hasOwn(problemMessages, error.code)
      ? problemMessages[error.code]
      : undefined
  return {
    title: message?.title ?? 'Не удалось подтвердить запуск',
    description:
      message?.description ??
      'Повторите отправку с теми же данными, чтобы проверить результат запуска.',
    requestId: error instanceof ApiError ? error.requestId : undefined,
    refreshSources:
      error instanceof ApiProblemError && error.code === 'SOURCE_NOT_AVAILABLE',
  }
}

function hasText(value: string) {
  return /[^\p{White_Space}]/u.test(value)
}

export function useInvestigationForm() {
  const lifetime = new AbortController()
  onScopeDispose(() => lifetime.abort())

  const type = ref<SubjectInput['type']>('person')
  const name = ref('')
  const identifierType = ref('')
  const identifier = ref('')
  const selectedCodes = ref<string[]>([])
  const catalog = shallowRef<SourceCatalog>()
  const catalogLoading = ref(false)
  const catalogError = shallowRef<{ requestId?: string }>()
  const submitting = ref(false)
  const formError = ref('')
  const submitError = shallowRef<ReturnType<typeof submissionError>>()
  const accepted = shallowRef<InvestigationAccepted>()
  const keys = new Map<string, string>()

  const sources = computed(() =>
    (catalog.value?.items ?? []).filter(
      (source) =>
        source.enabled &&
        source.roles.includes('discovery') &&
        source.entity_types.includes(type.value),
    ),
  )

  async function loadSources() {
    if (catalogLoading.value || submitting.value) return
    catalogLoading.value = true
    catalogError.value = undefined
    try {
      const result = await listSources(lifetime.signal)
      if (lifetime.signal.aborted) return
      catalog.value = result
      selectedCodes.value = selectedCodes.value.filter((code) =>
        sources.value.some((source) => source.code === code),
      )
      if (submitError.value?.refreshSources) submitError.value = undefined
    } catch (error) {
      if (!lifetime.signal.aborted) {
        catalogError.value = {
          requestId: error instanceof ApiError ? error.requestId : undefined,
        }
      }
    } finally {
      catalogLoading.value = false
    }
  }

  onMounted(loadSources)

  async function submit() {
    if (
      submitting.value ||
      accepted.value ||
      catalogLoading.value ||
      catalogError.value
    )
      return
    formError.value = ''
    submitError.value = undefined
    const inputType = type.value
    if (inputType !== 'person' && inputType !== 'organization') {
      formError.value = 'Выберите тип субъекта.'
      return
    }
    const inputName = hasText(name.value) ? name.value : undefined
    const hasIdentifierType = hasText(identifierType.value)
    const hasIdentifier = hasText(identifier.value)
    if (
      [name.value, identifierType.value, identifier.value].some((value) =>
        value.includes('\0'),
      ) ||
      (inputName && [...inputName].length > 500)
    ) {
      formError.value =
        'Имя должно содержать до 500 символов. Удалите нулевые символы из полей.'
      return
    }
    if (hasIdentifierType !== hasIdentifier) {
      formError.value = 'Укажите тип и значение идентификатора.'
      return
    }
    const identifiers = hasIdentifier
      ? { [identifierType.value]: identifier.value }
      : undefined
    let subject: SubjectInput
    if (inputName) {
      subject = { type: inputType, name: inputName, identifiers }
    } else if (identifiers) {
      subject = { type: inputType, identifiers }
    } else {
      formError.value = 'Укажите имя, название или известный идентификатор.'
      return
    }
    const codes = sources.value
      .filter(({ code }) => selectedCodes.value.includes(code))
      .map(({ code }) => code)
      .sort()
    if (!codes.length) {
      formError.value = 'Выберите хотя бы один доступный источник.'
      return
    }

    const request: CreateInvestigationRequest = {
      subject,
      limits: { sources: codes },
    }
    const fingerprint = JSON.stringify(request)
    submitting.value = true
    try {
      const key = keys.get(fingerprint) ?? createUuid()
      keys.set(fingerprint, key)
      const result = await createInvestigation(request, key, lifetime.signal)
      if (!lifetime.signal.aborted) {
        accepted.value = result
        return result
      }
    } catch (error) {
      if (!lifetime.signal.aborted) {
        submitError.value = submissionError(error)
      }
    } finally {
      submitting.value = false
    }
  }

  return {
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
  }
}
