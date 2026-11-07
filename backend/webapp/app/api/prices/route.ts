import { NextResponse } from 'next/server'
import { getStreamPriceSnapshot } from '@/lib/redis'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET() {
  try {
    const rows = await getStreamPriceSnapshot()
    return NextResponse.json({ data: rows, ts: Date.now() })
  } catch (error) {
    console.error('[prices API]', error)
    return NextResponse.json({ error: 'Failed to load prices' }, { status: 500 })
  }
}
