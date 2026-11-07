import { getAllSpreads } from '@/lib/redis'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

const INTERVAL_MS = parseInt(process.env.STREAM_INTERVAL_MS || '2000')

export async function GET() {
  const encoder = new TextEncoder()
  let closed = false

  const stream = new ReadableStream({
    async start(controller) {
      function send(payload: unknown) {
        if (closed) return
        try {
          controller.enqueue(encoder.encode(`data: ${JSON.stringify(payload)}\n\n`))
        } catch {
          closed = true
        }
      }

      // Send initial data immediately
      try {
        const spreads = await getAllSpreads()
        send({ type: 'spreads', data: spreads, count: spreads.length, ts: Date.now() })
      } catch (err) {
        console.error('[spread stream]', err)
        send({ type: 'error', message: 'Failed to load spreads' })
      }

      // Poll on interval
      const timer = setInterval(async () => {
        if (closed) {
          clearInterval(timer)
          return
        }
        try {
          const spreads = await getAllSpreads()
          send({ type: 'spreads', data: spreads, count: spreads.length, ts: Date.now() })
        } catch (err) {
          console.error('[spread stream]', err)
          send({ type: 'error', message: 'Failed to load spreads' })
        }
      }, INTERVAL_MS)

      return () => {
        closed = true
        clearInterval(timer)
      }
    },
    cancel() {
      closed = true
    },
  })

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    },
  })
}
