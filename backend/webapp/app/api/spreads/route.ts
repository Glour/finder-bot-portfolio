import { NextResponse } from 'next/server'
import { getAllSpreads } from '@/lib/redis'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET() {
  try {
    const spreads = await getAllSpreads()
    return NextResponse.json({
      data: spreads,
      count: spreads.length,
      ts: Date.now(),
    })
  } catch (error) {
    console.error('[API /spreads] error:', error)
    return NextResponse.json(
      { error: 'Failed to fetch spreads from Redis' },
      { status: 500 }
    )
  }
}
