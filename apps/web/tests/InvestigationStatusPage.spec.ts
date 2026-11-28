import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import App from '@/App.vue'
import { router } from '@/app/router'
import type { InvestigationStatus } from '@/shared/api'

const request = vi.hoisted(() => {
  const mock = vi.fn<typeof fetch>()
  vi.stubGlobal('fetch', mock)
  return mock
})
const id = '10000000-0000-4000-8000-000000000024'
const nextId = '10000000-0000-4000-8000-000000000025'
const path = (value = id) => `/investigations/${value}/status`
const timestamp = '2026-11-28T08:30:00Z'
const nextAttempt = '2026-11-28T11:35:00+03:00'

function statusPayload(
  overrides: Partial<InvestigationStatus> = {},
): InvestigationStatus {
  const sources = overrides.sources ?? []
  return {
    id,
    status: 'running',
    stage: 'subject_discovery',
    state_version: 1,
    progress: {
      active_jobs: sources.reduce(
        (count, row) => count + row.active_job_count,
        0,
      ),
      terminal_jobs: sources.reduce(
        (count, row) => count + row.terminal_job_count,
        0,
      ),
      known_jobs: sources.reduce(
        (count, row) => count + row.known_job_count,
        0,
      ),
    },
    partial_result: {
      succeeded_jobs: sources.reduce(
        (count, row) => count + row.succeeded_job_count,
        0,
      ),
      sources_with_results: sources.filter((row) => row.succeeded_job_count > 0)
        .length,
    },
    sources,
    requires_action: false,
    pending_review_count: 0,
    reviews_url: null,
    source_actions: [],
    updated_at: timestamp,
    ...overrides,
  }
}

function status(overrides: Partial<InvestigationStatus> = {}) {
  return Response.json(statusPayload(overrides))
}

type Source = InvestigationStatus['sources'][number]

function source(
  state: Source['status'],
  overrides: Partial<Source> = {},
): Source {
  const active = [
    'pending',
    'running',
    'retry_wait',
    'action_required',
  ].includes(state)
  const jobs = state === 'completed_partial' ? 2 : 1
  return {
    source_code: state,
    status: state,
    active_job_count: active ? 1 : 0,
    terminal_job_count: active ? 0 : jobs,
    known_job_count: jobs,
    succeeded_job_count:
      state === 'succeeded' || state === 'completed_partial' ? 1 : 0,
    attempt_count: overrides.known_job_count ?? jobs,
    error_code: null,
    error_detail: null,
    next_attempt_at: null,
    updated_at: timestamp,
    ...overrides,
  }
}

function problem(httpStatus: number) {
  return Response.json(
    {
      type: 'about:blank',
      title: 'Internal title',
      code: 'TEST_ERROR',
      detail: 'Internal detail',
      status: httpStatus,
    },
    {
      status: httpStatus,
      headers: {
        'Content-Type': 'application/problem+json',
        'X-Request-ID': 'status-request',
      },
    },
  )
}

function mountApp() {
  return mount(App, { global: { plugins: [router] } })
}

describe('Статус расследования', () => {
  let wrapper: ReturnType<typeof mountApp> | undefined
  let hidden: boolean

  beforeEach(async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    hidden = false
    vi.spyOn(document, 'hidden', 'get').mockImplementation(() => hidden)
    request.mockReset()
    await router.replace(path())
  })

  afterEach(() => {
    wrapper?.unmount()
    wrapper = undefined
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('показывает загрузку и пустой список источников', async () => {
    request.mockResolvedValueOnce(status())
    wrapper = mountApp()
    expect(wrapper.get('[aria-busy=true]').text()).toContain('Загружаем статус')
    await flushPromises()
    expect(wrapper.find('[aria-busy=true]').exists()).toBe(false)
    expect(wrapper.text()).toContain('Источников пока нет')
    expect(wrapper.find('#result-summary-title').exists()).toBe(false)
    expect(request.mock.calls.map(([url]) => url)).toEqual([`/api/v1${path()}`])
  })

  it('показывает все агрегированные состояния, результат и детали попыток до завершения', async () => {
    request.mockResolvedValueOnce(
      status({
        sources: [
          source('pending'),
          source('running', {
            active_job_count: 2,
            terminal_job_count: 1,
            known_job_count: 3,
            succeeded_job_count: 1,
            next_attempt_at: nextAttempt,
          }),
          source('retry_wait', {
            attempt_count: 2,
            next_attempt_at: nextAttempt,
            error_code: 'TIMEOUT',
            error_detail: 'Источник не ответил вовремя.',
          }),
          source('action_required'),
          source('succeeded'),
          source('not_found'),
          source('completed_partial'),
          source('unavailable', { error_code: 'MAINTENANCE' }),
          source('failed'),
          source('cancelled'),
        ],
      }),
    )
    wrapper = mountApp()
    await flushPromises()
    expect(wrapper.text()).toContain('8 из 13 известных')
    expect(
      wrapper
        .get('[aria-labelledby=result-summary-title]')
        .findAll('dd')
        .map((item) => item.text()),
    ).toEqual(['3', '3'])
    const sources = wrapper.get('section[aria-labelledby=source-status-title]')
    const rows = sources.findAll('li')
    expect(rows.map((row) => row.get('dd').text())).toEqual([
      'Ожидает запуска',
      'В работе',
      'Ожидает повторной попытки',
      'Требуется действие',
      'Результат получен',
      'Ничего не найдено',
      'Часть результатов получена',
      'Недоступен',
      'Ошибка',
      'Отменён',
    ])
    const retryWait = rows.find((row) => row.get('h3').text() === 'retry_wait')!
    const running = rows.find((row) => row.get('h3').text() === 'running')!
    expect(running.findAll('time').at(1)?.attributes('datetime')).toBe(
      nextAttempt,
    )
    expect(retryWait.findAll('dd').at(1)?.text()).toBe('2')
    expect(
      retryWait.findAll('time').map((time) => time.attributes('datetime')),
    ).toEqual([timestamp, nextAttempt])
    expect(retryWait.text()).toContain('Источник не ответил вовремя.')
    expect(retryWait.text()).not.toContain('TIMEOUT')
    expect(sources.text()).toContain('MAINTENANCE')
  })

  it('показывает результаты, пока у того же источника остаются активные задания', async () => {
    const fixture = source('running', {
      source_code: 'fixture',
      active_job_count: 3,
      known_job_count: 3,
    })
    request
      .mockResolvedValueOnce(status({ sources: [fixture] }))
      .mockResolvedValueOnce(
        status({
          sources: [
            {
              ...fixture,
              active_job_count: 1,
              terminal_job_count: 2,
              succeeded_job_count: 2,
            },
          ],
        }),
      )
    wrapper = mountApp()
    await flushPromises()
    expect(wrapper.find('#result-summary-title').exists()).toBe(false)
    await vi.advanceTimersByTimeAsync(2500)

    expect(
      wrapper
        .get('[aria-labelledby=result-summary-title]')
        .findAll('dd')
        .map((item) => item.text()),
    ).toEqual(['2', '1'])
    const row = wrapper.get('.source-status-list li')
    expect(row.get('dd').text()).toBe('В работе')
    expect(
      row
        .findAll('dd')
        .map((item) => item.text())
        .slice(0, 3),
    ).toEqual(['В работе', '3', '2'])
    expect(wrapper.text()).toContain('2 из 3 известных')
  })

  it('сохраняет последний результат при сбоях, увеличивает задержку и сбрасывает её после ответа', async () => {
    request
      .mockResolvedValueOnce(status({ sources: [source('succeeded')] }))
      .mockResolvedValueOnce(problem(503))
      .mockRejectedValueOnce(new TypeError('Network error'))
      .mockResolvedValueOnce(status())
      .mockResolvedValueOnce(status({ status: 'completed' }))
    wrapper = mountApp()
    await flushPromises()
    await vi.advanceTimersByTimeAsync(2499)
    expect(request).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1)
    const alert = wrapper.get('[role=alert]')
    expect(alert.text()).toContain('Не удалось обновить прогресс')
    expect(alert.text()).toContain('status-request')
    expect(alert.text()).not.toContain('Internal detail')
    expect(
      wrapper
        .get('[aria-labelledby=result-summary-title]')
        .findAll('dd')
        .map((item) => item.text()),
    ).toEqual(['1', '1'])

    await vi.advanceTimersByTimeAsync(4999)
    expect(request).toHaveBeenCalledTimes(2)
    await vi.advanceTimersByTimeAsync(1)
    expect(request).toHaveBeenCalledTimes(3)
    await vi.advanceTimersByTimeAsync(9999)
    expect(request).toHaveBeenCalledTimes(3)
    await vi.advanceTimersByTimeAsync(1)
    expect(wrapper.find('[role=alert]').exists()).toBe(false)

    await vi.advanceTimersByTimeAsync(2500)
    expect(request).toHaveBeenCalledTimes(5)
  })

  it.each(['completed', 'completed_partial', 'failed', 'cancelled'] as const)(
    'замедляет polling в скрытой вкладке и останавливает его при %s',
    async (terminalState) => {
      request
        .mockResolvedValueOnce(status())
        .mockResolvedValueOnce(status())
        .mockResolvedValueOnce(status({ status: terminalState }))
      wrapper = mountApp()
      await flushPromises()
      hidden = true
      document.dispatchEvent(new Event('visibilitychange'))
      await vi.advanceTimersByTimeAsync(12499)
      expect(request).toHaveBeenCalledTimes(1)
      await vi.advanceTimersByTimeAsync(1)
      expect(request).toHaveBeenCalledTimes(2)
      hidden = false
      document.dispatchEvent(new Event('visibilitychange'))
      await vi.advanceTimersByTimeAsync(2500)
      expect(request).toHaveBeenCalledTimes(3)
      document.dispatchEvent(new Event('visibilitychange'))
      await vi.advanceTimersByTimeAsync(120_000)
      expect(request).toHaveBeenCalledTimes(3)
    },
  )

  it('сохраняет отсчёт от последнего ответа при повторных сменах видимости', async () => {
    request.mockResolvedValueOnce(status()).mockResolvedValueOnce(status())
    wrapper = mountApp()
    await flushPromises()

    await vi.advanceTimersByTimeAsync(2000)
    hidden = true
    document.dispatchEvent(new Event('visibilitychange'))
    await vi.advanceTimersByTimeAsync(2000)
    expect(request).toHaveBeenCalledTimes(1)
    hidden = false
    document.dispatchEvent(new Event('visibilitychange'))
    await vi.advanceTimersByTimeAsync(1)
    expect(request).toHaveBeenCalledTimes(2)
  })

  it.each([
    [404, 'Расследование не найдено.'],
    [501, 'Просмотр прогресса пока недоступен.'],
  ] as const)(
    'показывает ошибку %i без повторного запроса',
    async (code, detail) => {
      request.mockResolvedValueOnce(problem(code))
      wrapper = mountApp()
      await flushPromises()
      expect(wrapper.get('[role=alert]').text()).toContain(detail)
      await vi.advanceTimersByTimeAsync(120_000)
      expect(request).toHaveBeenCalledTimes(1)
    },
  )

  it('позволяет сразу повторить временно неудачный запрос', async () => {
    request.mockResolvedValueOnce(problem(503)).mockResolvedValueOnce(status())
    wrapper = mountApp()
    await flushPromises()
    expect(wrapper.get('[role=alert]').text()).toContain('Статус недоступен')
    await wrapper.get('[role=alert] button').trigger('click')
    await flushPromises()
    expect(wrapper.find('[role=alert]').exists()).toBe(false)
    expect(request).toHaveBeenCalledTimes(2)
  })

  it.each([
    { status: 'incomplete' },
    { stage: 'unknown' },
    { progress: { active_jobs: 6, terminal_jobs: -1, known_jobs: 5 } },
    { progress: { active_jobs: 0, terminal_jobs: 3, known_jobs: 2 } },
    { progress: { active_jobs: 2, terminal_jobs: 0, known_jobs: 2 } },
    { partial_result: undefined },
    { partial_result: { succeeded_jobs: -1, sources_with_results: 0 } },
    { partial_result: { succeeded_jobs: 1, sources_with_results: 0 } },
    { partial_result: { succeeded_jobs: 0, sources_with_results: 1 } },
    { sources: [source('running', { succeeded_job_count: 1 })] },
    {
      sources: [
        source('succeeded', {
          active_job_count: 1,
          terminal_job_count: 0,
          succeeded_job_count: 0,
        }),
      ],
    },
    {
      progress: { active_jobs: 0, terminal_jobs: 1, known_jobs: 1 },
      sources: [source('succeeded', { succeeded_job_count: 0 })],
    },
    { sources: [{ ...source('running'), active_job_count: undefined }] },
    { sources: [source('running', { known_job_count: 0 })] },
    {
      progress: { active_jobs: 2, terminal_jobs: 0, known_jobs: 2 },
      sources: [source('running'), source('running')],
    },
    { sources: [source('running', { attempt_count: -1 })] },
    { sources: [{ ...source('running'), status: 'queued' }] },
    { sources: [source('retry_wait')] },
    { sources: [source('pending', { next_attempt_at: nextAttempt })] },
    {
      progress: { active_jobs: 0, terminal_jobs: 1, known_jobs: 1 },
      sources: [source('not_found', { next_attempt_at: nextAttempt })],
    },
    { sources: [source('retry_wait', { next_attempt_at: 'tomorrow' })] },
    { sources: [source('running', { updated_at: '2026-11-28T08:30:60Z' })] },
    {
      sources: [
        source('retry_wait', { next_attempt_at: '2026-02-29T00:00:00Z' }),
      ],
    },
  ])(
    'сохраняет снимок и продолжает опрос после некорректного ответа %j',
    async (overrides) => {
      request
        .mockResolvedValueOnce(
          status({
            sources: [
              source('running', {
                terminal_job_count: 1,
                known_job_count: 2,
                succeeded_job_count: 1,
              }),
            ],
          }),
        )
        .mockResolvedValueOnce(
          Response.json({
            ...statusPayload({
              stage: 'enrichment',
              sources: [source('running')],
            }),
            ...overrides,
          }),
        )
        .mockResolvedValueOnce(
          status({ status: 'completed', sources: [source('succeeded')] }),
        )
      wrapper = mountApp()
      await flushPromises()
      await vi.advanceTimersByTimeAsync(2500)
      expect(wrapper.get('[role=alert]').text()).toContain(
        'Не удалось обновить прогресс',
      )
      expect(wrapper.text()).toContain('1 из 2 известных')
      expect(
        wrapper
          .get('[aria-labelledby=result-summary-title]')
          .findAll('dd')
          .map((item) => item.text()),
      ).toEqual(['1', '1'])
      await vi.advanceTimersByTimeAsync(4999)
      expect(request).toHaveBeenCalledTimes(2)
      await vi.advanceTimersByTimeAsync(1)
      expect(wrapper.find('[role=alert]').exists()).toBe(false)
      expect(wrapper.text()).toContain('1 из 1 известных')
      await vi.advanceTimersByTimeAsync(120_000)
      expect(request).toHaveBeenCalledTimes(3)
    },
  )

  it('отменяет зависший запрос и возобновляет опрос с задержкой', async () => {
    request
      .mockImplementationOnce(
        (_, options) =>
          new Promise((_, reject) => {
            const signal = options?.signal
            signal?.addEventListener('abort', () => reject(signal.reason), {
              once: true,
            })
          }),
      )
      .mockResolvedValueOnce(status())
    wrapper = mountApp()
    await vi.advanceTimersByTimeAsync(14_999)
    expect(wrapper.find('[aria-busy=true]').exists()).toBe(true)
    expect(request).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1)
    expect(request.mock.calls[0]?.[1]?.signal?.aborted).toBe(true)
    expect(wrapper.find('[role=alert]').exists()).toBe(true)
    await vi.advanceTimersByTimeAsync(5000)
    expect(request).toHaveBeenCalledTimes(2)
    expect(wrapper.find('[role=alert]').exists()).toBe(false)
  })

  it('отменяет запрос при смене id и выходе со страницы и игнорирует запоздалый ответ', async () => {
    let resolvePrevious!: (response: Response) => void
    request
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolvePrevious = resolve
          }),
      )
      .mockImplementationOnce(() => new Promise(() => {}))
    wrapper = mountApp()
    await vi.advanceTimersByTimeAsync(2500)
    expect(request).toHaveBeenCalledTimes(1)
    await router.push(path(nextId))
    expect(request.mock.calls[0]?.[1]?.signal?.aborted).toBe(true)
    expect(request.mock.calls[1]?.[0]).toBe(`/api/v1${path(nextId)}`)
    resolvePrevious(status({ sources: [source('succeeded')] }))
    await flushPromises()
    expect(wrapper.find('#result-summary-title').exists()).toBe(false)
    expect(wrapper.find('[aria-busy=true]').exists()).toBe(true)
    wrapper.unmount()
    wrapper = undefined
    expect(request.mock.calls[1]?.[1]?.signal?.aborted).toBe(true)
    await vi.advanceTimersByTimeAsync(120_000)
    expect(request).toHaveBeenCalledTimes(2)
  })
})
