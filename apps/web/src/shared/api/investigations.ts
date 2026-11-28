import { apiClient } from './client'
import { ApiError } from './errors'
import type {
  CreateInvestigationRequest,
  InvestigationAccepted,
  InvestigationStatusSnapshot,
  SourceCatalog,
} from './types'

const investigationStates = new Set<InvestigationStatusSnapshot['status']>([
  'created',
  'running',
  'completed',
  'completed_partial',
  'failed',
  'cancelled',
])
const investigationStages = new Set<InvestigationStatusSnapshot['stage']>([
  'subject_discovery',
  'subject_resolution',
  'enrichment',
  'analysis',
  'completed',
])
const uuidPattern = /^[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}$/i
// Status timestamps use Go time.Time, which cannot represent leap seconds.
const dateTimePattern =
  /^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/i

function isDateTime(value: unknown): boolean {
  if (typeof value !== 'string') return false
  if (dateTimePattern.exec(value)?.[0] !== value) return false
  const day = value.slice(0, 10)
  // Date rolls an invalid day of the month into the next month.
  return new Date(`${day}T00:00:00Z`).getUTCDate() === Number(day.slice(-2))
}

function isCount(value: number): boolean {
  return Number.isInteger(value) && value >= 0
}

function hasConsistentSourceState(
  source: InvestigationStatusSnapshot['sources'][number],
): boolean {
  const {
    status,
    active_job_count,
    terminal_job_count,
    succeeded_job_count,
    next_attempt_at,
  } = source
  if (active_job_count === 0 && next_attempt_at !== null) return false
  switch (status) {
    case 'pending':
      return active_job_count > 0 && next_attempt_at === null
    case 'retry_wait':
      return active_job_count > 0 && next_attempt_at !== null
    case 'running':
    case 'action_required':
      return active_job_count > 0
    case 'succeeded':
      return active_job_count === 0 && succeeded_job_count > 0
    case 'completed_partial':
      return (
        active_job_count === 0 &&
        succeeded_job_count > 0 &&
        succeeded_job_count < terminal_job_count
      )
    case 'not_found':
    case 'unavailable':
    case 'failed':
    case 'cancelled':
      return (
        active_job_count === 0 &&
        terminal_job_count > 0 &&
        succeeded_job_count === 0
      )
    default:
      return false
  }
}

function hasConsistentSourceTotals(
  status: InvestigationStatusSnapshot,
): boolean {
  if (!Array.isArray(status.sources)) return false
  const sourceCodes = new Set<string>()
  let activeJobs = 0
  let terminalJobs = 0
  let succeededJobs = 0
  let sourcesWithResults = 0
  for (const source of status.sources) {
    if (
      !source ||
      typeof source.source_code !== 'string' ||
      source.source_code.length === 0 ||
      sourceCodes.has(source.source_code) ||
      !isCount(source.active_job_count) ||
      !isCount(source.terminal_job_count) ||
      !isCount(source.known_job_count) ||
      source.active_job_count + source.terminal_job_count !==
        source.known_job_count ||
      !isCount(source.succeeded_job_count) ||
      source.succeeded_job_count > source.terminal_job_count ||
      !hasConsistentSourceState(source) ||
      !isCount(source.attempt_count) ||
      !isDateTime(source.updated_at) ||
      (source.next_attempt_at !== null &&
        !isDateTime(source.next_attempt_at)) ||
      (source.error_detail !== null &&
        typeof source.error_detail !== 'string') ||
      (source.error_code !== null && typeof source.error_code !== 'string')
    ) {
      return false
    }
    sourceCodes.add(source.source_code)
    activeJobs += source.active_job_count
    terminalJobs += source.terminal_job_count
    succeededJobs += source.succeeded_job_count
    if (source.succeeded_job_count > 0) sourcesWithResults += 1
  }
  return (
    activeJobs === status.progress.active_jobs &&
    terminalJobs === status.progress.terminal_jobs &&
    succeededJobs === status.partial_result.succeeded_jobs &&
    sourcesWithResults === status.partial_result.sources_with_results
  )
}

export async function getInvestigationStatus(
  id: string,
  signal: AbortSignal,
): Promise<InvestigationStatusSnapshot> {
  const status = await apiClient.request<InvestigationStatusSnapshot>(
    `/investigations/${encodeURIComponent(id)}/status`,
    { expectedStatus: 200, signal, cache: 'no-store' },
  )
  const partialResult = status?.partial_result
  if (
    typeof status?.id !== 'string' ||
    !uuidPattern.test(status.id) ||
    status.id.toLowerCase() !== id.toLowerCase() ||
    !investigationStates.has(status.status) ||
    !investigationStages.has(status.stage) ||
    !status.progress ||
    !isCount(status.progress.terminal_jobs) ||
    !isCount(status.progress.known_jobs) ||
    !isCount(status.progress.active_jobs) ||
    status.progress.active_jobs + status.progress.terminal_jobs !==
      status.progress.known_jobs ||
    !partialResult ||
    !isCount(partialResult.succeeded_jobs) ||
    !isCount(partialResult.sources_with_results) ||
    !hasConsistentSourceTotals(status)
  ) {
    throw new ApiError({
      status: 200,
      code: 'invalid_investigation_status',
      title: 'Некорректный ответ API',
      detail: 'Не удалось прочитать статус расследования.',
    })
  }
  return status
}

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
    !uuidPattern.test(accepted.id) ||
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
