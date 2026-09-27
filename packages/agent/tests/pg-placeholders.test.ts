import { describe, expect, it } from 'vitest'
import { toPgPlaceholders } from '../src/db-client.js'

describe('toPgPlaceholders', () => {
  it('translates ? placeholders to $1..$n in order', () => {
    expect(
      toPgPlaceholders('INSERT INTO t (a, b, c) VALUES (?, ?, ?)'),
    ).toBe('INSERT INTO t (a, b, c) VALUES ($1, $2, $3)')
  })

  it('leaves a statement that already uses $n untouched', () => {
    const sql = 'SELECT * FROM t WHERE a = $1 AND b = $2'
    expect(toPgPlaceholders(sql)).toBe(sql)
  })

  it('handles no placeholders', () => {
    expect(toPgPlaceholders('SELECT 1')).toBe('SELECT 1')
  })

  it('translates placeholders inside a larger statement (WHERE + VALUES)', () => {
    expect(
      toPgPlaceholders(
        'UPDATE t SET x = ? WHERE tenant = ? AND name = ? RETURNING id',
      ),
    ).toBe('UPDATE t SET x = $1 WHERE tenant = $2 AND name = $3 RETURNING id')
  })
})
