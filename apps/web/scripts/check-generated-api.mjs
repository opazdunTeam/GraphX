import { execFileSync } from 'node:child_process'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const webRoot = fileURLToPath(new URL('../', import.meta.url))
const specPath = resolve(webRoot, '../../contracts/openapi.yaml')
const generatedPath = resolve(webRoot, 'src/shared/api/generated/openapi.ts')
const tempDirectory = mkdtempSync(resolve(tmpdir(), 'graphx-openapi-check-'))
const tempOutput = resolve(tempDirectory, 'openapi.ts')

try {
  const pnpmCli = process.env.npm_execpath
  if (!pnpmCli) {
    throw new Error('check:api must be run through pnpm')
  }

  execFileSync(
    process.execPath,
    [pnpmCli, 'exec', 'openapi-typescript', specPath, '-o', tempOutput],
    {
      cwd: webRoot,
      stdio: 'pipe',
    },
  )

  const committed = readFileSync(generatedPath, 'utf8')
  const regenerated = readFileSync(tempOutput, 'utf8')
  if (committed !== regenerated) {
    throw new Error('generated OpenAPI types are stale; run pnpm generate:api')
  }

  const assertions = {
    'SubjectInput is an explicit union': committed.includes(
      'SubjectInput: components["schemas"]["NamedSubjectInput"] | components["schemas"]["IdentifiedSubjectInput"];',
    ),
    'SubjectInput is not weakened to unknown':
      !/SubjectInput:[^;]*unknown/.test(committed),
    'depth remains optional': committed.includes('depth?: number;'),
    'entity_limit remains optional': committed.includes(
      'entity_limit?: number;',
    ),
    'dynamic artifact media type is generated':
      committed.includes('"*/*": string;'),
  }

  for (const [name, passed] of Object.entries(assertions)) {
    if (!passed) {
      throw new Error(`generated TypeScript assertion failed: ${name}`)
    }
  }

  console.log(
    'generated TypeScript API is current and preserves the required contract shape',
  )
} finally {
  rmSync(tempDirectory, { recursive: true, force: true })
}
