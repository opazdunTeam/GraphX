import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import App from '@/App.vue'
import { router } from '@/app/router'
import type { InvestigationStatus, SourceCatalog } from '@/shared/api'

const request = vi.hoisted(() => {
  const mock = vi.fn<typeof fetch>()
  vi.stubGlobal('fetch', mock)
  return mock
})

const id = '10000000-0000-4000-8000-000000000023'
const path = `/investigations/${id}/status`
const fixture: SourceCatalog['items'][number] = {
  code: 'fixture',
  name: 'Fixture',
  enabled: true,
  mode: 'file',
  roles: ['discovery'],
  entity_types: ['person'],
  health: {
    state: 'unknown',
    observed_at: null,
    last_success_at: null,
    error_code: null,
    error_detail: null,
  },
}

function catalog(items: SourceCatalog['items'] = [fixture]) {
  const response: SourceCatalog = { items, updated_at: '2026-11-27T00:00:00Z' }
  return Response.json(response)
}

function acceptance(status = 202) {
  return Response.json(
    {
      id,
      status: 'running',
      stage: 'subject_discovery',
      status_url: `/api/v1${path}`,
    },
    { status },
  )
}

function problem(code: string, status = 400) {
  return Response.json(
    { type: 'about:blank', code, status, title: code, detail: 'API detail' },
    {
      status,
      headers: {
        'Content-Type': 'application/problem+json',
        'X-Request-ID': 'api-request',
      },
    },
  )
}

function posts() {
  return request.mock.calls
    .filter(([, options]) => options?.method === 'POST')
    .map(([, options]) => ({
      key: new Headers(options?.headers).get('Idempotency-Key'),
      body: options?.body,
    }))
}

function mountApp() {
  return mount(App, { global: { plugins: [router] } })
}

describe('Запуск расследования', () => {
  let wrapper: ReturnType<typeof mountApp>

  beforeEach(async () => {
    request.mockReset()
    await router.replace('/')
  })

  afterEach(() => wrapper.unmount())

  it('отправляет имя и выбранный источник, применяет серверные лимиты и открывает статус после 202', async () => {
    request.mockResolvedValueOnce(
      catalog([
        fixture,
        { ...fixture, code: 'disabled', enabled: false },
        { ...fixture, code: 'enrichment', roles: ['enrichment'] },
        { ...fixture, code: 'organization', entity_types: ['organization'] },
      ]),
    )
    let accept!: (response: Response) => void
    request.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          accept = resolve
        }),
    )
    const initialStatus: InvestigationStatus = {
      id,
      status: 'running',
      stage: 'subject_discovery',
      state_version: 1,
      progress: { active_jobs: 1, terminal_jobs: 0, known_jobs: 1 },
      partial_result: { succeeded_jobs: 0, sources_with_results: 0 },
      sources: [
        {
          source_code: 'fixture',
          status: 'pending',
          active_job_count: 1,
          terminal_job_count: 0,
          known_job_count: 1,
          succeeded_job_count: 0,
          attempt_count: 1,
          error_code: null,
          error_detail: null,
          next_attempt_at: null,
          updated_at: '2026-11-27T00:00:00Z',
        },
      ],
      requires_action: false,
      pending_review_count: 0,
      reviews_url: null,
      source_actions: [],
      updated_at: '2026-11-27T00:00:00Z',
    }
    request.mockResolvedValueOnce(Response.json(initialStatus))
    wrapper = mountApp()
    await flushPromises()
    expect(request.mock.calls[0]?.[0]).toBe('/api/v1/sources')
    expect(
      wrapper
        .findAll('input[type=checkbox]')
        .map((input) => input.attributes('value')),
    ).toEqual(['fixture'])
    await wrapper.get('[name=subject-name]').setValue('  Ада Лавлейс  ')
    await wrapper.get('input[value=fixture]').setValue(true)
    await wrapper.get('form').trigger('submit')
    await wrapper.get('form').trigger('submit')
    expect(posts()).toHaveLength(1)
    expect(posts()[0]?.key).toMatch(/^[a-f0-9-]{36}$/)
    expect(JSON.parse(String(posts()[0]?.body))).toEqual({
      subject: { type: 'person', name: '  Ада Лавлейс  ' },
      limits: { sources: ['fixture'] },
    })
    expect(router.currentRoute.value.path).toBe('/')
    accept(acceptance())
    await flushPromises()
    expect(router.currentRoute.value.path).toBe(path)
    expect(wrapper.get('h1').text()).toBe('Статус расследования')
    expect(wrapper.text()).toContain('Принято в работу')
    expect(request.mock.calls.at(-1)?.[0]).toBe(`/api/v1${path}`)
    expect(wrapper.text()).toContain('0 из 1 известных')
    expect(wrapper.get('.source-status-list').text()).toContain(
      'Ожидает запуска',
    )
    expect(wrapper.find('[role=alert]').exists()).toBe(false)
    expect(wrapper.text()).toContain(id)
  })

  it('проверяет поля и поддерживает организацию с одним идентификатором при подходящем каталоге', async () => {
    request
      .mockResolvedValueOnce(
        catalog([
          fixture,
          { ...fixture, code: 'registry', entity_types: ['organization'] },
        ]),
      )
      .mockResolvedValueOnce(acceptance())
    wrapper = mountApp()
    await flushPromises()
    await wrapper.get('input[value=fixture]').setValue(true)
    await wrapper.get('select').setValue('organization')
    expect(wrapper.find('input[value=fixture]').exists()).toBe(false)
    await wrapper.get('form').trigger('submit')
    expect(wrapper.text()).toContain(
      'Укажите имя, название или известный идентификатор',
    )
    await wrapper.get('[name=identifier-type]').setValue('bin')
    await wrapper.get('form').trigger('submit')
    expect(wrapper.text()).toContain('Укажите тип и значение идентификатора')
    await wrapper.get('[name=identifier-value]').setValue('001234567890')
    await wrapper.get('form').trigger('submit')
    expect(wrapper.text()).toContain('Выберите хотя бы один доступный источник')
    expect(posts()).toHaveLength(0)
    await wrapper.get('input[value=registry]').setValue(true)
    await wrapper.get('form').trigger('submit')
    await flushPromises()
    expect(JSON.parse(String(posts()[0]?.body))).toEqual({
      subject: { type: 'organization', identifiers: { bin: '001234567890' } },
      limits: { sources: ['registry'] },
    })
  })

  it('сохраняет ключ после сетевого сбоя и неожиданного 200 и меняет его для другой команды', async () => {
    request
      .mockResolvedValueOnce(catalog())
      .mockRejectedValueOnce(new TypeError('network unavailable'))
      .mockResolvedValueOnce(acceptance(200))
      .mockResolvedValueOnce(problem('INVALID_REQUEST'))
      .mockResolvedValueOnce(acceptance())
    wrapper = mountApp()
    await flushPromises()
    const name = wrapper.get('[name=subject-name]')
    await name.setValue('Ада')
    await wrapper.get('input[value=fixture]').setValue(true)
    for (const value of ['Ада', 'Ада', 'Грейс']) {
      await name.setValue(value)
      await wrapper.get('form').trigger('submit')
      await flushPromises()
      expect(router.currentRoute.value.path).toBe('/')
    }
    expect(wrapper.text()).toContain('Проверьте данные расследования')
    expect(wrapper.text()).not.toContain('Источник недоступен для запуска')
    await name.setValue('Ада')
    await wrapper.get('form').trigger('submit')
    await flushPromises()
    const [first, second, changed, restored] = posts()
    expect(first).toEqual(second)
    expect(first).toEqual(restored)
    expect(first?.key).not.toBe(changed?.key)
    expect(router.currentRoute.value.path).toBe(path)
  })

  it('показывает ошибку источника отдельно, обновляет каталог и снимает недоступный выбор', async () => {
    request
      .mockResolvedValueOnce(catalog())
      .mockResolvedValueOnce(problem('SOURCE_NOT_AVAILABLE'))
      .mockResolvedValueOnce(catalog([{ ...fixture, enabled: false }]))
    wrapper = mountApp()
    await flushPromises()
    await wrapper.get('[name=subject-name]').setValue('Ада')
    await wrapper.get('input[value=fixture]').setValue(true)
    await wrapper.get('form').trigger('submit')
    await flushPromises()
    expect(wrapper.text()).toContain('Источник недоступен для запуска')
    expect(wrapper.text()).not.toContain('Проверьте данные расследования')
    expect(wrapper.text()).toContain('api-request')
    await wrapper.get('.error-state button').trigger('click')
    await flushPromises()
    expect(wrapper.find('input[type=checkbox]').exists()).toBe(false)
    expect(wrapper.text()).toContain('Нет доступных источников')
    expect(wrapper.get('button[type=submit]').attributes()).toHaveProperty(
      'disabled',
    )
  })

  it('повторно загружает каталог после ошибки и не предлагает Fixture для организации', async () => {
    request
      .mockResolvedValueOnce(problem('INTERNAL_ERROR', 500))
      .mockResolvedValueOnce(catalog())
    wrapper = mountApp()
    await flushPromises()
    expect(wrapper.text()).toContain('Не удалось загрузить источники')
    await wrapper.get('.error-state button').trigger('click')
    await flushPromises()
    expect(wrapper.find('input[value=fixture]').exists()).toBe(true)
    await wrapper.get('select').setValue('organization')
    expect(wrapper.text()).toContain('Нет доступных источников')
    expect(wrapper.find('input[type=checkbox]').exists()).toBe(false)
    expect(posts()).toHaveLength(0)
  })
})
