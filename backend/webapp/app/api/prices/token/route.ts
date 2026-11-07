import { NextRequest, NextResponse } from 'next/server'
import { getStreamPriceSnapshot } from '@/lib/redis'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET(req: NextRequest) {
  const id = req.nextUrl.searchParams.get('id')
  if (!id) return NextResponse.json({ error: 'missing id' }, { status: 400 })

  try {
    const rows = await getStreamPriceSnapshot()
    const row = rows.find((r) => r.global_token_id === id) ?? null
    return NextResponse.json({ data: row, ts: Date.now() })
  } catch (error) {
    console.error('[price token API]', error)
    return NextResponse.json({ error: 'Failed to load token prices' }, { status: 500 })
  }
}
