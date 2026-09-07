import pino from 'pino'

export type Logger = pino.Logger

/**
 * Process-wide structured logger. JSON by default (k8s-friendly); set
 * LOG_LEVEL to raise/lower verbosity. Child loggers add session/component
 * bindings without breaking the JSON shape.
 */
export const logger: Logger = pino({
  name: 'easylab-agent',
  level: process.env.LOG_LEVEL ?? 'info',
})

export function childLogger(bindings: Record<string, unknown>): Logger {
  return logger.child(bindings)
}
