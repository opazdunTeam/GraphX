import { describe, expect, it, vi } from 'vitest'

import {
  ApiError,
  ApiProblemError,
  createApiClient,
  createUuid,
} from '@/shared/api'

function fetchMock(response: Response): ReturnType<typeof vi.fn<typeof fetch>> {
  return vi.fn<typeof fetch>().mockResolvedValue(response)
}

describe('API client', () => {
  it('can generate UUIDs on a LAN HTTP connection', () => {
    const browserCrypto = globalThis.crypto
    vi.stubGlobal('crypto', {
      getRandomValues: browserCrypto.getRandomValues.bind(browserCrypto),
    })
    try {
      expect(createUuid()).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
      )
    } finally {
      vi.unstubAllGlobals()
    }
  })
  it('joins the base URL and sends controlled request headers and JSON', async () => {
    const request = fetchMock(
      new Response(JSON.stringify({ accepted: true }), {
        status: 202,
        headers: {
          'Content-Type': 'application/json',
          'X-Request-ID': 'server-request',
        },
      }),
    )
    const client = createApiClient({
      baseUrl: 'https://api.example.test/api/v1/',
      fetch: request,
      requestIdFactory: () => 'generated-request',
    })

    await client.request('/investigations', {
      method: 'POST',
      json: { subject: { type: 'person', name: 'Ada' } },
      idempotencyKey: '34c9169c-3ce2-4746-9b95-2fbbc7b99035',
    })

    expect(request).toHaveBeenCalledOnce()
    const [url, init] = request.mock.calls[0] ?? []
    const headers = new Headers(init?.headers)
    expect(url).toBe('https://api.example.test/api/v1/investigations')
    expect(init?.body).toBe(
      JSON.stringify({ subject: { type: 'person', name: 'Ada' } }),
    )
    expect(headers.get('Accept')).toBe(
      'application/json, application/problem+json',
    )
    expect(headers.get('Content-Type')).toBe('application/json')
    expect(headers.get('X-Request-ID')).toBe('generated-request')
    expect(headers.get('Idempotency-Key')).toBe(
      '34c9169c-3ce2-4746-9b95-2fbbc7b99035',
    )
  })

  it('returns typed JSON and accepts a content type parameter', async () => {
    const client = createApiClient({
      fetch: fetchMock(
        new Response(JSON.stringify({ status: 'running' }), {
          headers: { 'Content-Type': 'application/json; charset=utf-8' },
        }),
      ),
      requestIdFactory: () => 'request-id',
    })

    const result = await client.request<{ status: string }>('/investigations/1')

    expect(result).toEqual({ status: 'running' })
  })

  it('returns undefined for an empty successful response', async () => {
    const client = createApiClient({
      fetch: fetchMock(new Response(null, { status: 204 })),
      requestIdFactory: () => 'request-id',
    })

    await expect(client.request<void>('/commands/1')).resolves.toBeUndefined()
  })

  it.each([
    ['invalid_json_response', '{', 'application/json'],
    ['unexpected_content_type', 'ok', 'text/plain'],
  ])(
    'rejects an invalid successful response as %s',
    async (code, body, contentType) => {
      const client = createApiClient({
        fetch: fetchMock(
          new Response(body, { headers: { 'Content-Type': contentType } }),
        ),
        requestIdFactory: () => 'request-id',
      })

      const error = await client
        .request('/sources')
        .catch((cause: unknown) => cause)

      expect(error).toBeInstanceOf(ApiError)
      expect(error).toMatchObject({ code, requestId: 'request-id' })
    },
  )

  it('supports a custom response parser for non-JSON endpoints', async () => {
    const client = createApiClient({
      fetch: fetchMock(
        new Response('artifact', { headers: { 'Content-Type': 'text/plain' } }),
      ),
      requestIdFactory: () => 'request-id',
    })

    await expect(
      client.request('/artifacts/1/content', {
        parse: (response) => response.text(),
      }),
    ).resolves.toBe('artifact')
  })

  it('converts a valid problem response into ApiProblemError', async () => {
    const problem = {
      type: 'https://graphx.test/problems/invalid-request',
      title: 'Некорректный запрос',
      status: 400,
      code: 'invalid_request',
      detail: 'Проверьте введённые данные.',
    }
    const client = createApiClient({
      fetch: fetchMock(
        new Response(JSON.stringify(problem), {
          status: 400,
          headers: {
            'Content-Type': 'application/problem+json; charset=utf-8',
            'X-Request-ID': 'server-request',
          },
        }),
      ),
      requestIdFactory: () => 'client-request',
    })

    const error = await client
      .request('/sources')
      .catch((cause: unknown) => cause)

    expect(error).toBeInstanceOf(ApiProblemError)
    expect(error).toMatchObject({
      status: 400,
      code: 'invalid_request',
      detail: 'Проверьте введённые данные.',
      requestId: 'server-request',
    })
  })

  it.each([
    ['malformed JSON', '{', 'application/problem+json'],
    [
      'invalid problem shape',
      JSON.stringify({ status: 400 }),
      'application/problem+json',
    ],
    ['non-problem body', 'internal stack trace: secret', 'text/plain'],
  ])('uses a safe fallback for %s', async (_name, body, contentType) => {
    const client = createApiClient({
      fetch: fetchMock(
        new Response(body, {
          status: 500,
          headers: { 'Content-Type': contentType },
        }),
      ),
      requestIdFactory: () => 'request-id',
    })

    const error = await client
      .request('/sources')
      .catch((cause: unknown) => cause)

    expect(error).toBeInstanceOf(ApiError)
    expect(error).not.toBeInstanceOf(ApiProblemError)
    expect((error as ApiError).detail).not.toContain('secret')
    expect((error as ApiError).code).toMatch(
      /^(invalid_problem_response|unexpected_error_response)$/,
    )
  })

  it('does not mask network errors or request cancellation', async () => {
    const networkError = new TypeError('network unavailable')
    const cancelled = new DOMException('cancelled', 'AbortError')
    const networkClient = createApiClient({
      fetch: vi.fn<typeof fetch>().mockRejectedValue(networkError),
      requestIdFactory: () => 'request-id',
    })
    const cancelledClient = createApiClient({
      fetch: vi.fn<typeof fetch>().mockRejectedValue(cancelled),
      requestIdFactory: () => 'request-id',
    })

    await expect(networkClient.request('/sources')).rejects.toBe(networkError)
    await expect(cancelledClient.request('/sources')).rejects.toBe(cancelled)
  })

  it('forwards AbortSignal without implementing hidden retries', async () => {
    const request = fetchMock(
      new Response(JSON.stringify({ items: [] }), {
        headers: { 'Content-Type': 'application/json' },
      }),
    )
    const controller = new AbortController()
    const client = createApiClient({
      fetch: request,
      requestIdFactory: () => 'request-id',
    })

    await client.request('/sources', { signal: controller.signal })

    expect(request).toHaveBeenCalledOnce()
    expect(request.mock.calls[0]?.[1]?.signal).toBe(controller.signal)
  })
})
