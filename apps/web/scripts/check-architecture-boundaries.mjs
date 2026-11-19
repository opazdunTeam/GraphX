import { ESLint } from 'eslint'

const eslint = new ESLint()
const filePath = 'src/pages/architecture-boundary-probe.ts'
const boundaryRule = 'graphx/api-boundary'

const probes = [
  {
    name: 'public alias import',
    source:
      "import { createApiClient } from '@/shared/api'\nexport const client = createApiClient()",
  },
  {
    name: 'public relative import',
    source:
      "import { createApiClient } from '../../shared/api'\nexport const client = createApiClient()",
  },
  {
    name: 'public dynamic import',
    source: "export const api = import('@/shared/api')",
  },
  {
    name: 'fetch method on a local object',
    source:
      "const self = { fetch: (url: string) => url }\nexport const request = () => self.fetch('/api/v1/sources')",
  },
  {
    name: 'dynamic template import outside API boundary',
    source:
      "const moduleName = 'details'\nexport const feature = import(`../../features/${moduleName}`)",
  },
  {
    name: 'bare fetch',
    source: "export const request = () => fetch('/api/v1/sources')",
    expectedRule: 'no-restricted-globals',
  },
  ...['globalThis', 'self', 'window'].flatMap((browserGlobal) => [
    {
      name: `${browserGlobal}.fetch`,
      source: `export const request = () => ${browserGlobal}.fetch('/api/v1/sources')`,
      expectedRule: boundaryRule,
    },
    {
      name: `${browserGlobal}['fetch']`,
      source: `export const request = () => ${browserGlobal}['fetch']('/api/v1/sources')`,
      expectedRule: boundaryRule,
    },
    {
      name: `destructured ${browserGlobal}.fetch`,
      source: `const { fetch: request } = ${browserGlobal}\nexport { request }`,
      expectedRule: boundaryRule,
    },
  ]),
  {
    name: 'alias internal import',
    source:
      "import { createApiClient } from '@/shared/api/client'\nexport { createApiClient }",
    expectedRule: boundaryRule,
  },
  {
    name: 'relative internal import',
    source:
      "import { createApiClient } from '../../shared/api/client'\nexport { createApiClient }",
    expectedRule: boundaryRule,
  },
  {
    name: 'relative generated import',
    source:
      "import type { components } from '../../shared/api/generated/openapi'\nexport type Problem = components['schemas']['Problem']",
    expectedRule: boundaryRule,
  },
  {
    name: 'internal re-export',
    source: "export * from '@/shared/api/client'",
    expectedRule: boundaryRule,
  },
  {
    name: 'alias dynamic internal import',
    source: "export const client = import('@/shared/api/client')",
    expectedRule: boundaryRule,
  },
  {
    name: 'relative dynamic internal import',
    source: "export const client = import('../../shared/api/client')",
    expectedRule: boundaryRule,
  },
  {
    name: 'constant template dynamic internal import',
    source: 'void import(`../../shared/api/${"client"}`)',
    expectedRule: boundaryRule,
  },
  {
    name: 'variable template dynamic internal import',
    source:
      "const moduleName = 'client'\nvoid import(`../../shared/api/${moduleName}`)",
    expectedRule: boundaryRule,
  },
]

for (const probe of probes) {
  const [result] = await eslint.lintText(probe.source, { filePath })
  if (!result) {
    throw new Error(
      `ESLint returned no result for architecture probe: ${probe.name}`,
    )
  }

  const errorRules = result.messages
    .filter((message) => message.severity === 2)
    .map((message) => message.ruleId)

  if (probe.expectedRule) {
    if (!errorRules.includes(probe.expectedRule)) {
      throw new Error(
        `${probe.name} must fail ${probe.expectedRule}; received: ${errorRules.join(', ') || 'no errors'}`,
      )
    }
  } else if (errorRules.length > 0) {
    throw new Error(
      `${probe.name} must pass; received: ${errorRules.join(', ')}`,
    )
  }
}

console.log(
  `frontend API architecture boundaries are enforced by ESLint (${probes.length} probes)`,
)
