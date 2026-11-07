import { NextResponse } from 'next/server'
import { getStreamPriceSnapshot } from '@/lib/redis'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET() {
  try {
    const rows = await getStreamPriceSnapshot()
    return NextResponse.json({ data: rows, ts: Date.now() })
  } catch (error) {
    return NextResponse.json({ error: String(error) }, { status: 500 })
  }
}
