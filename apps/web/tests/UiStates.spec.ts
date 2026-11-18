import { mount } from '@vue/test-utils'
import { describe, expect, it } from 'vitest'

import { AppLayout, EmptyState, ErrorState, LoadingState } from '@/shared/ui'

describe('shared UI states', () => {
  it('renders application content inside the common layout', () => {
    const wrapper = mount(AppLayout, {
      global: {
        stubs: {
          RouterLink: { template: '<a><slot /></a>' },
        },
      },
      slots: {
        default: '<section>Рабочая область</section>',
        navigation: '<a href="/investigations">Расследования</a>',
      },
    })

    expect(wrapper.get('header').text()).toContain('GraphX')
    expect(wrapper.get('nav').attributes('aria-label')).toBe(
      'Основная навигация',
    )
    expect(wrapper.get('main').text()).toContain('Рабочая область')
  })

  it('announces loading without implying progress percentage', () => {
    const wrapper = mount(LoadingState, {
      props: { label: 'Загружаем источники…' },
    })

    expect(wrapper.attributes('role')).toBe('status')
    expect(wrapper.attributes('aria-busy')).toBe('true')
    expect(wrapper.text()).toBe('Загружаем источники…')
  })

  it('renders an empty state and optional action', () => {
    const wrapper = mount(EmptyState, {
      props: {
        title: 'Результатов пока нет',
        description: 'Источники ещё выполняются.',
      },
      slots: { actions: '<button>Обновить</button>' },
    })

    expect(wrapper.attributes('role')).toBe('status')
    expect(wrapper.text()).toContain('Результатов пока нет')
    expect(wrapper.get('button').text()).toBe('Обновить')
  })

  it('renders a safe error and emits retry explicitly', async () => {
    const wrapper = mount(ErrorState, {
      props: {
        description: 'Не удалось получить состояние.',
        requestId: 'request-123',
        retryable: true,
      },
    })

    expect(wrapper.attributes('role')).toBe('alert')
    expect(wrapper.text()).toContain('request-123')
    await wrapper.get('button').trigger('click')
    expect(wrapper.emitted('retry')).toHaveLength(1)
  })

  it('does not render a retry control unless the caller enables it', () => {
    const wrapper = mount(ErrorState, {
      props: { description: 'Запрос не выполнен.' },
    })

    expect(wrapper.find('button').exists()).toBe(false)
  })
})
