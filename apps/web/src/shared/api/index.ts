import type { components } from './generated/openapi'

export { apiBaseUrl } from './client'
export type { components, operations, paths } from './generated/openapi'

export type Problem = components['schemas']['Problem']
export type SubjectInput = components['schemas']['SubjectInput']
export type InvestigationLimits = components['schemas']['InvestigationLimits']
