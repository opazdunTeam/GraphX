import eslint from '@eslint/js'
import vue from 'eslint-plugin-vue'
import globals from 'globals'
import tseslint from 'typescript-eslint'
import apiBoundary from './eslint-rules/api-boundary.mjs'

export default tseslint.config(
  { ignores: ['dist/**', 'node_modules/**', 'src/shared/api/generated/**'] },
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  ...vue.configs['flat/recommended'],
  {
    files: [
      'eslint.config.js',
      'eslint-rules/**/*.mjs',
      'scripts/**/*.mjs',
      'vite.config.ts',
    ],
    languageOptions: {
      globals: globals.node,
    },
  },
  {
    files: ['**/*.{ts,vue}'],
    languageOptions: {
      globals: globals.browser,
      parserOptions: {
        parser: tseslint.parser,
      },
    },
    rules: {
      'vue/html-self-closing': ['warn', { html: { void: 'always' } }],
      'vue/max-attributes-per-line': 'off',
      'vue/multi-word-component-names': 'off',
      'vue/singleline-html-element-content-newline': 'off',
    },
  },
  {
    files: ['src/**/*.{ts,vue}'],
    ignores: ['src/shared/api/**/*'],
    plugins: {
      graphx: {
        rules: {
          'api-boundary': apiBoundary,
        },
      },
    },
    rules: {
      'graphx/api-boundary': 'error',
      'no-restricted-globals': [
        'error',
        {
          name: 'fetch',
          message:
            'Use the shared/api client so errors and request metadata stay consistent.',
        },
      ],
    },
  },
)
