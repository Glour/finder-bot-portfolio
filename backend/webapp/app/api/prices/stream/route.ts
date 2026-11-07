import { getStreamPriceSnapshot } from '@/lib/redis'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

const INTERVAL_MS = parseInt(process.env.STREAM_INTERVAL_MS || '3000')

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

      try {
        const rows = await getStreamPriceSnapshot()
        send({ type: 'prices', data: rows, ts: Date.now() })
      } catch (err) {
        send({ type: 'error', message: String(err) })
      }

      const timer = setInterval(async () => {
        if (closed) { clearInterval(timer); return }
        try {
          const rows = await getStreamPriceSnapshot()
          send({ type: 'prices', data: rows, ts: Date.now() })
        } catch (err) {
          send({ type: 'error', message: String(err) })
        }
      }, INTERVAL_MS)

      return () => { closed = true; clearInterval(timer) }
    },
    cancel() { closed = true },
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
