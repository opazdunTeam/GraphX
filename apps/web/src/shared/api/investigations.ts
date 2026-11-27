import { apiClient } from './client'
import { ApiError } from './errors'
import type {
  CreateInvestigationRequest,
  InvestigationAccepted,
  SourceCatalog,
} from './types'

export async function listSources(signal: AbortSignal): Promise<SourceCatalog> {
  const catalog = await apiClient.request<SourceCatalog>('/sources', {
    expectedStatus: 200,
    signal,
  })
  if (
    !Array.isArray(catalog?.items) ||
    !catalog.items.every(
      (source) =>
        source &&
        typeof source.code === 'string' &&
        typeof source.name === 'string' &&
        typeof source.enabled === 'boolean' &&
        Array.isArray(source.roles) &&
        Array.isArray(source.entity_types),
    )
  ) {
    throw new ApiError({
      status: 200,
      code: 'invalid_source_catalog',
      title: 'Некорректный ответ API',
      detail: 'Не удалось прочитать каталог источников.',
    })
  }
  return catalog
}

export async function createInvestigation(
  request: CreateInvestigationRequest,
  idempotencyKey: string,
  signal: AbortSignal,
): Promise<InvestigationAccepted> {
  const accepted = await apiClient.request<InvestigationAccepted>(
    '/investigations',
    {
      method: 'POST',
      expectedStatus: 202,
      json: request,
      idempotencyKey,
      signal,
    },
  )
  if (
    !accepted ||
    typeof accepted.id !== 'string' ||
    !/^[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}$/i.test(accepted.id) ||
    accepted.status !== 'running' ||
    accepted.stage !== 'subject_discovery' ||
    typeof accepted.status_url !== 'string' ||
    !accepted.status_url
  ) {
    throw new ApiError({
      status: 202,
      code: 'invalid_investigation_response',
      title: 'Некорректный ответ API',
      detail: 'Не удалось прочитать подтверждение запуска расследования.',
    })
  }
  return accepted
}
