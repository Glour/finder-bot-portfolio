import type { TickerInfo, SpreadSignal } from './types'

export interface ExchangePrice {
  bid: number | null
  ask: number | null
  get_at: number | null
  symbol: string
}

export interface TokenPriceRow {
  global_token_id: string
  symbol: string
  exchanges: Record<string, ExchangePrice>
}

export function buildPriceMatrix(signals: SpreadSignal[]): TokenPriceRow[] {
  const byToken = new Map<string, { symbol: string; exchanges: Record<string, ExchangePrice> }>()

  for (const signal of signals) {
    if (!signal) continue
    const { global_token_id, symbol, tickers_data } = signal
    if (!global_token_id || !tickers_data) continue

    if (!byToken.has(global_token_id)) {
      byToken.set(global_token_id, { symbol, exchanges: {} })
    }
    const row = byToken.get(global_token_id)!

    const types: Array<[string, Record<string, TickerInfo>]> = [
      ['spot', tickers_data.spot || {}],
      ['futures', tickers_data.futures || {}],
      ['dex', tickers_data.dex || {}],
    ]

    for (const [marketType, group] of types) {
      for (const [exchangeName, ticker] of Object.entries(group)) {
        const key = `${marketType}_${exchangeName}`
        if (!ticker) continue

        const bids = ticker.orderbook?.bids || []
        const asks = ticker.orderbook?.asks || []
        const bid = bids.length > 0 ? bids[0][0] : null
        const ask = asks.length > 0 ? asks[0][0] : null
        const get_at = ticker.get_at || null

        const existing = row.exchanges[key]
        if (!existing || (get_at && (!existing.get_at || get_at > existing.get_at))) {
          row.exchanges[key] = { bid, ask, get_at, symbol: ticker.symbol || '' }
        }
      }
    }
  }

  return Array.from(byToken.entries())
    .map(([global_token_id, data]) => ({ global_token_id, ...data }))
    .sort((a, b) => {
      const aCount = Object.values(a.exchanges).filter((e) => e.bid !== null).length
      const bCount = Object.values(b.exchanges).filter((e) => e.bid !== null).length
      if (bCount !== aCount) return bCount - aCount
      return a.symbol.localeCompare(b.symbol)
    })
}
