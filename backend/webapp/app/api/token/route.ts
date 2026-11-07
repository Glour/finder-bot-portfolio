import { NextRequest, NextResponse } from 'next/server'
import { getTokenDetail } from '@/lib/db'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET(req: NextRequest) {
  const id = req.nextUrl.searchParams.get('id')
  if (!id) return NextResponse.json({ error: 'missing id' }, { status: 400 })

  try {
    const detail = await getTokenDetail(id)
    return NextResponse.json({ data: detail, ts: Date.now() })
  } catch (error) {
    console.error('[TokenDetail] error:', error)
    return NextResponse.json({ error: 'Failed to load token details' }, { status: 500 })
  }
}
