import { NextResponse } from 'next/server'
import { getPool } from '@/lib/db'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET() {
  try {
    const pool = getPool()
    const { rows } = await pool.query(`
      SELECT
        tb.id,
        COALESCE(gt.symbol, tb.contract_address, 'Unknown') AS symbol,
        tb.mute_type,
        tb.expires_at,
        tb.chain_name,
        tb.contract_address,
        tb.created_at,
        tb.global_token_id
      FROM token_blacklist tb
      LEFT JOIN global_tokens gt ON gt.id = tb.global_token_id
      ORDER BY tb.created_at DESC
    `)
    return NextResponse.json({ data: rows })
  } catch (e) {
    console.error('[mutes API]', e)
    return NextResponse.json({ error: 'Failed to load mutes' }, { status: 500 })
  }
}
