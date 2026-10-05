import { mount } from '@vue/test-utils'
import { describe, expect, it } from 'vitest'

import HomePage from '@/pages/home/HomePage.vue'

describe('HomePage', () => {
  it('renders the scaffold status', () => {
    const wrapper = mount(HomePage)

    expect(wrapper.get('h1').text()).toBe('GraphX')
    expect(wrapper.text()).toContain('Web')
    expect(wrapper.text()).toContain('/api/v1')
  })
})
