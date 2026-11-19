const browserGlobalNames = new Set(['globalThis', 'self', 'window'])

const directFetchMessage =
  'Use the shared/api client so errors and request metadata stay consistent.'
const internalImportMessage =
  'Use the public @/shared/api boundary instead of its internals.'

function staticString(node) {
  if (!node) {
    return undefined
  }

  if (node.type === 'Literal' && typeof node.value === 'string') {
    return node.value
  }

  if (node.type === 'TemplateLiteral') {
    let value = ''

    for (const [index, quasi] of node.quasis.entries()) {
      value += quasi.value.cooked ?? quasi.value.raw

      const expression = node.expressions[index]
      if (expression) {
        const expressionValue = staticString(expression)
        if (expressionValue === undefined) {
          return undefined
        }
        value += expressionValue
      }
    }

    return value
  }

  if (node.type === 'BinaryExpression' && node.operator === '+') {
    const left = staticString(node.left)
    const right = staticString(node.right)
    return left === undefined || right === undefined ? undefined : left + right
  }

  return undefined
}

function propertyName(property, computed) {
  if (!computed && property.type === 'Identifier') {
    return property.name
  }

  return staticString(property)
}

function isBrowserGlobal(context, node) {
  if (node.type !== 'Identifier' || !browserGlobalNames.has(node.name)) {
    return false
  }

  let scope = context.sourceCode.getScope(node)
  while (scope) {
    const variable = scope.variables.find(({ name }) => name === node.name)
    if (variable) {
      return variable.defs.length === 0
    }
    scope = scope.upper
  }

  return false
}

function isInternalApiPath(node) {
  const path = staticString(node)?.replaceAll('\\', '/')
  if (path !== undefined) {
    return /(?:^|\/)shared\/api\/.+/.test(path)
  }

  if (node.type !== 'TemplateLiteral' || node.expressions.length === 0) {
    return false
  }

  const staticPath = node.quasis
    .map(({ value }) => value.cooked ?? value.raw)
    .join('')
    .replaceAll('\\', '/')

  // A dynamic segment can extend the public barrel into an internal module.
  // Treat an explicit shared/api path in the static template shape as internal.
  return /(?:^|\/)shared\/api(?:\/|$)/.test(staticPath)
}

function reportDestructuredFetch(context, pattern, value) {
  if (pattern.type !== 'ObjectPattern' || !isBrowserGlobal(context, value)) {
    return
  }

  for (const property of pattern.properties) {
    if (
      property.type === 'Property' &&
      propertyName(property.key, property.computed) === 'fetch'
    ) {
      context.report({ node: property, messageId: 'directFetch' })
    }
  }
}

const apiBoundaryRule = {
  meta: {
    type: 'problem',
    docs: {
      description:
        'Prevent direct browser fetch calls and imports of shared API internals.',
    },
    schema: [],
    messages: {
      directFetch: directFetchMessage,
      internalImport: internalImportMessage,
    },
  },
  create(context) {
    function reportInternalImport(node) {
      if (node.source && isInternalApiPath(node.source)) {
        context.report({ node: node.source, messageId: 'internalImport' })
      }
    }

    return {
      AssignmentExpression(node) {
        reportDestructuredFetch(context, node.left, node.right)
      },
      AssignmentPattern(node) {
        reportDestructuredFetch(context, node.left, node.right)
      },
      ExportAllDeclaration: reportInternalImport,
      ExportNamedDeclaration: reportInternalImport,
      ImportDeclaration: reportInternalImport,
      ImportExpression(node) {
        if (isInternalApiPath(node.source)) {
          context.report({ node: node.source, messageId: 'internalImport' })
        }
      },
      MemberExpression(node) {
        if (
          isBrowserGlobal(context, node.object) &&
          propertyName(node.property, node.computed) === 'fetch'
        ) {
          context.report({ node, messageId: 'directFetch' })
        }
      },
      VariableDeclarator(node) {
        if (node.init) {
          reportDestructuredFetch(context, node.id, node.init)
        }
      },
    }
  },
}

export default apiBoundaryRule
