import { NextRequest, NextResponse } from 'next/server'
import { getPool } from '@/lib/db'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET(req: NextRequest) {
  const q = req.nextUrl.searchParams.get('q')?.trim() || ''
  if (q.length < 6) return NextResponse.json({ ids: [] })

  try {
    const pool = getPool()
    // Search by contract address (partial match, case-insensitive)
    const { rows } = await pool.query(
      `SELECT DISTINCT tc.global_token_id::text
       FROM token_chains tc
       WHERE LOWER(tc.contract_address) LIKE LOWER($1)
         AND tc.global_token_id IS NOT NULL
       LIMIT 50`,
      [`%${q}%`]
    )
    return NextResponse.json({ ids: rows.map((r: { global_token_id: string }) => r.global_token_id) })
  } catch (e) {
    console.error('[search API]', e)
    return NextResponse.json({ ids: [] })
  }
}
