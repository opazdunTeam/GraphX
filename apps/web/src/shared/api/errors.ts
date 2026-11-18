import type { Problem } from './types'

interface ApiErrorOptions {
  cause?: unknown
  code: string
  detail: string
  requestId?: string
  status: number
  title: string
}

function isProblem(value: unknown): value is Problem {
  if (typeof value !== 'object' || value === null) {
    return false
  }
  const candidate = value as Record<string, unknown>

  return (
    typeof candidate.type === 'string' &&
    typeof candidate.title === 'string' &&
    typeof candidate.status === 'number' &&
    Number.isInteger(candidate.status) &&
    candidate.status >= 400 &&
    candidate.status <= 599 &&
    typeof candidate.code === 'string' &&
    candidate.code.length > 0 &&
    typeof candidate.detail === 'string' &&
    candidate.detail.length > 0 &&
    (candidate.instance === undefined || typeof candidate.instance === 'string')
  )
}

export class ApiError extends Error {
  readonly code: string
  readonly detail: string
  readonly requestId?: string
  readonly status: number
  readonly title: string

  constructor(options: ApiErrorOptions) {
    super(options.detail, { cause: options.cause })
    this.name = 'ApiError'
    this.code = options.code
    this.detail = options.detail
    this.requestId = options.requestId
    this.status = options.status
    this.title = options.title
  }

  static fromProblem(
    value: unknown,
    requestId?: string,
    expectedStatus?: number,
  ): ApiProblemError {
    if (
      !isProblem(value) ||
      (expectedStatus !== undefined && value.status !== expectedStatus)
    ) {
      throw new ApiError({
        status: expectedStatus ?? 500,
        code: 'invalid_problem_response',
        title: 'Некорректная ошибка API',
        detail: 'Сервер вернул ошибку в неподдерживаемом формате.',
        requestId,
      })
    }

    return new ApiProblemError(value, requestId)
  }
}

export class ApiProblemError extends ApiError {
  readonly problem: Problem

  constructor(problem: Problem, requestId?: string) {
    super({
      status: problem.status,
      code: problem.code,
      title: problem.title,
      detail: problem.detail,
      requestId,
    })
    this.name = 'ApiProblemError'
    this.problem = problem
  }
}
