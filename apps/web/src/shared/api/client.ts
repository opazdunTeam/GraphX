import { ApiError } from './errors'
import { createUuid } from './uuid'

export const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? '/api/v1'

export type ApiResponseParser<T> = (response: Response) => Promise<T>

export interface ApiRequestOptions<T> extends Omit<
  RequestInit,
  'body' | 'headers'
> {
  headers?: HeadersInit
  expectedStatus?: number
  idempotencyKey?: string
  json?: unknown
  parse?: ApiResponseParser<T>
  requestId?: string
}

export interface ApiClient {
  request<T>(path: string, options?: ApiRequestOptions<T>): Promise<T>
}

export interface CreateApiClientOptions {
  baseUrl?: string
  fetch?: typeof fetch
  requestIdFactory?: () => string
}

const JSON_MEDIA_TYPE = 'application/json'
const PROBLEM_MEDIA_TYPE = 'application/problem+json'

function mediaType(response: Response): string {
  return (
    response.headers
      .get('Content-Type')
      ?.split(';', 1)[0]
      ?.trim()
      .toLowerCase() ?? ''
  )
}

function joinApiUrl(baseUrl: string, path: string): string {
  const normalizedBaseUrl = baseUrl.replace(/\/+$/, '')
  const normalizedPath = path.replace(/^\/+/, '')

  return normalizedPath
    ? `${normalizedBaseUrl}/${normalizedPath}`
    : normalizedBaseUrl
}

async function parseJson<T>(response: Response, requestId: string): Promise<T> {
  const body = await response.text()
  if (!body) {
    return undefined as T
  }
  if (mediaType(response) !== JSON_MEDIA_TYPE) {
    throw new ApiError({
      status: response.status,
      code: 'unexpected_content_type',
      title: 'Неожиданный формат ответа',
      detail: 'Сервер вернул успешный ответ в неподдерживаемом формате.',
      requestId,
    })
  }

  try {
    return JSON.parse(body) as T
  } catch (cause) {
    throw new ApiError({
      status: response.status,
      code: 'invalid_json_response',
      title: 'Некорректный ответ API',
      detail: 'Сервер вернул повреждённый JSON.',
      requestId,
      cause,
    })
  }
}

async function responseError(
  response: Response,
  fallbackRequestId: string,
): Promise<ApiError> {
  const requestId = response.headers.get('X-Request-ID') ?? fallbackRequestId
  if (mediaType(response) === PROBLEM_MEDIA_TYPE) {
    try {
      return ApiError.fromProblem(
        await response.json(),
        requestId,
        response.status,
      )
    } catch (cause) {
      if (cause instanceof ApiError) {
        return cause
      }
      return new ApiError({
        status: response.status,
        code: 'invalid_problem_response',
        title: 'Некорректная ошибка API',
        detail: 'Сервер вернул ошибку в неподдерживаемом формате.',
        requestId,
        cause,
      })
    }
  }

  return new ApiError({
    status: response.status,
    code: 'unexpected_error_response',
    title: 'Ошибка запроса',
    detail: 'Сервер вернул непредусмотренный ответ об ошибке.',
    requestId,
  })
}

export function createApiClient(
  options: CreateApiClientOptions = {},
): ApiClient {
  const baseUrl = options.baseUrl ?? apiBaseUrl
  const fetchRequest = options.fetch ?? globalThis.fetch
  const requestIdFactory = options.requestIdFactory ?? createUuid

  return {
    async request<T>(
      path: string,
      requestOptions: ApiRequestOptions<T> = {},
    ): Promise<T> {
      const {
        headers: initialHeaders,
        expectedStatus,
        idempotencyKey,
        json,
        parse,
        requestId = requestIdFactory(),
        ...requestInit
      } = requestOptions
      const headers = new Headers(initialHeaders)
      if (!headers.has('Accept')) {
        headers.set('Accept', `${JSON_MEDIA_TYPE}, ${PROBLEM_MEDIA_TYPE}`)
      }
      headers.set('X-Request-ID', requestId)
      if (idempotencyKey) {
        headers.set('Idempotency-Key', idempotencyKey)
      }
      if (json !== undefined && !headers.has('Content-Type')) {
        headers.set('Content-Type', JSON_MEDIA_TYPE)
      }

      const response = await fetchRequest(joinApiUrl(baseUrl, path), {
        ...requestInit,
        body: json === undefined ? undefined : JSON.stringify(json),
        headers,
      })

      if (!response.ok) {
        throw await responseError(response, requestId)
      }
      if (expectedStatus !== undefined && response.status !== expectedStatus) {
        throw new ApiError({
          status: response.status,
          code: 'unexpected_response_status',
          title: 'Неожиданный ответ API',
          detail: 'Сервер не подтвердил выполнение запроса ожидаемым кодом.',
          requestId: response.headers.get('X-Request-ID') ?? requestId,
        })
      }
      if (parse) {
        return parse(response)
      }
      if (response.status === 204) {
        return undefined as T
      }

      return parseJson<T>(
        response,
        response.headers.get('X-Request-ID') ?? requestId,
      )
    },
  }
}

export const apiClient = createApiClient()
