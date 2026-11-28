import type { components } from './generated/openapi'

export type CreateInvestigationRequest =
  components['schemas']['CreateInvestigationRequest']
export type InvestigationAccepted =
  components['schemas']['InvestigationAccepted']
export type InvestigationLimits = components['schemas']['InvestigationLimits']
export type InvestigationStatus = components['schemas']['InvestigationStatus']
export type InvestigationStatusSnapshot = Pick<
  InvestigationStatus,
  'id' | 'status' | 'stage' | 'progress' | 'partial_result'
> & {
  sources: Pick<
    InvestigationStatus['sources'][number],
    | 'source_code'
    | 'status'
    | 'active_job_count'
    | 'terminal_job_count'
    | 'known_job_count'
    | 'succeeded_job_count'
    | 'attempt_count'
    | 'next_attempt_at'
    | 'error_code'
    | 'error_detail'
    | 'updated_at'
  >[]
}
export type Problem = components['schemas']['Problem']
export type SourceCatalog = components['schemas']['SourceCatalog']
export type SubjectInput = components['schemas']['SubjectInput']
