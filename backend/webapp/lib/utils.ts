import type { SpreadLevel, SpreadSignal, TickersData, TokenPrice } from './types'

// ── Price formatting ──────────────────────────────────────────────────────────

export function formatPrice(price: number | null): string {
  if (price === null || price === 0) return '—'
  if (price < 0.000001) return price.toExponential(4)
  if (price < 0.0001) return price.toFixed(8)
  if (price < 0.01) return price.toFixed(6)
  if (price < 1) return price.toFixed(5)
  if (price < 1000) return price.toFixed(4)
  return price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

export function formatSpread(spread: number): string {
  return `${spread.toFixed(2)}%`
}

export function formatProfit(profit: number): string {
  return `$${profit.toFixed(2)}`
}

export function formatVolume(volume: number): string {
  if (volume >= 1_000_000) return `${(volume / 1_000_000).toFixed(2)}M`
  if (volume >= 1_000) return `${(volume / 1_000).toFixed(2)}K`
  return volume.toFixed(2)
}

// ── Time ──────────────────────────────────────────────────────────────────────

export function getDataAgeMs(timestamp: number): number {
  return Date.now() - timestamp
}

export function formatAge(timestamp: number): string {
  const ms = getDataAgeMs(timestamp)
  const sec = Math.floor(ms / 1000)
  if (sec < 60) return `${sec}s`
  const min = Math.floor(sec / 60)
  if (min < 60) return `${min}m${sec % 60}s`
  return `${Math.floor(min / 60)}h${min % 60}m`
}

export function isStale(timestamp: number | null, thresholdMs = 60_000): boolean {
  if (!timestamp) return true
  return getDataAgeMs(timestamp) > thresholdMs
}

// ── Exchange ──────────────────────────────────────────────────────────────────

const EXCHANGE_LABELS: Record<string, string> = {
  gate: 'Gate.io',
  xt: 'XT',
  mexc: 'MEXC',
  bitmart: 'BitMart',
  lbank: 'LBank',
  hyperliquid: 'Hyperliquid',
  asterdex: 'AsterDEX',
  ascendex: 'AscendEX',
  gmgn: 'GMGN',
  bybit: 'Bybit',
  kucoin: 'KuCoin',
  binance: 'Binance',
  bitget: 'Bitget',
  okx: 'OKX',
}

const MARKET_LABELS: Record<string, string> = {
  spot: 'Spot',
  futures: 'Futures',
  dex: 'DEX',
}

const EXCHANGE_COLORS: Record<string, { bg: string; text: string; border: string }> = {
  gate: { bg: 'bg-blue-500/15', text: 'text-blue-400', border: 'border-blue-500/30' },
  xt: { bg: 'bg-orange-500/15', text: 'text-orange-400', border: 'border-orange-500/30' },
  mexc: { bg: 'bg-teal-500/15', text: 'text-teal-400', border: 'border-teal-500/30' },
  bitmart: { bg: 'bg-sky-500/15', text: 'text-sky-400', border: 'border-sky-500/30' },
  lbank: { bg: 'bg-violet-500/15', text: 'text-violet-400', border: 'border-violet-500/30' },
  hyperliquid: { bg: 'bg-emerald-500/15', text: 'text-emerald-400', border: 'border-emerald-500/30' },
  asterdex: { bg: 'bg-pink-500/15', text: 'text-pink-400', border: 'border-pink-500/30' },
  ascendex: { bg: 'bg-amber-500/15', text: 'text-amber-400', border: 'border-amber-500/30' },
  gmgn:    { bg: 'bg-purple-500/15',  text: 'text-purple-400',  border: 'border-purple-500/30' },
  bybit:   { bg: 'bg-yellow-500/15',  text: 'text-yellow-400',  border: 'border-yellow-500/30' },
  kucoin:  { bg: 'bg-green-500/15',   text: 'text-green-400',   border: 'border-green-500/30' },
  binance: { bg: 'bg-amber-400/15',   text: 'text-amber-300',   border: 'border-amber-400/30' },
  bitget:  { bg: 'bg-cyan-500/15',    text: 'text-cyan-400',    border: 'border-cyan-500/30' },
  okx:     { bg: 'bg-indigo-500/15',  text: 'text-indigo-400',  border: 'border-indigo-500/30' },
}

export function parseSourceKey(key: string | undefined | null): { market: string; exchange: string } {
  if (!key) return { market: 'spot', exchange: 'unknown' }
  const parts = key.split('_')
  if (parts.length < 2) return { market: 'spot', exchange: key }
  const market = parts[0]
  const exchange = parts.slice(1).join('_')
  return { market, exchange }
}

export function getExchangeLabel(sourceKey: string | undefined | null): string {
  const { market, exchange } = parseSourceKey(sourceKey)
  const exLabel = EXCHANGE_LABELS[exchange] || exchange.toUpperCase()
  const mktLabel = MARKET_LABELS[market] || market
  return `${exLabel} ${mktLabel}`
}

export function getExchangeColors(sourceKey: string | undefined | null) {
  const { exchange } = parseSourceKey(sourceKey)
  return (
    EXCHANGE_COLORS[exchange] || {
      bg: 'bg-slate-500/15',
      text: 'text-slate-400',
      border: 'border-slate-500/30',
    }
  )
}

export function getExchangeName(sourceKey: string | undefined | null): string {
  const { exchange } = parseSourceKey(sourceKey)
  return EXCHANGE_LABELS[exchange] || exchange.toUpperCase()
}

export function getMarketType(sourceKey: string | undefined | null): string {
  const { market } = parseSourceKey(sourceKey)
  return MARKET_LABELS[market] || market
}

// ── Exchange URLs ─────────────────────────────────────────────────────────────

// Chain name → GMGN chain slug
const GMGN_CHAIN_SLUGS: Record<string, string> = {
  'binance-smart-chain': 'bsc',
  'bsc': 'bsc',
  'bnb': 'bsc',
  'bnb smart chain': 'bsc',
  'ethereum': 'eth',
  'eth': 'eth',
  'solana': 'sol',
  'sol': 'sol',
  'base': 'base',
  'polygon': 'polygon',
  'polygon-pos': 'polygon',
  'arbitrum': 'arb',
  'arbitrum one': 'arb',
  'avalanche': 'avax',
  'avax': 'avax',
  'tron': 'tron',
  'sui': 'sui',
  'blast': 'blast',
}

export function getGmgnUrl(chainName: string, contractAddress: string): string | null {
  const slug = GMGN_CHAIN_SLUGS[chainName.toLowerCase()]
  if (!slug || !contractAddress) return null
  return `https://gmgn.ai/${slug}/token/${contractAddress}`
}

export function getExchangeUrl(sourceKey: string, symbol: string): string | null {
  const { market, exchange } = parseSourceKey(sourceKey)
  const sym = symbol.replace(/[/_]USDT$/i, '').toUpperCase()
  const symLow = sym.toLowerCase()

  switch (exchange) {
    case 'gate':
      if (market === 'spot')    return `https://www.gate.io/trade/${sym}_USDT`
      if (market === 'futures') return `https://www.gate.io/futures/USDT/${sym}_USDT`
      break
    case 'xt':
      if (market === 'spot')    return `https://www.xt.com/en/trade/${symLow}_usdt`
      if (market === 'futures') return `https://www.xt.com/en/futures/trade/${symLow}_usdt`
      break
    case 'mexc':
      if (market === 'futures') return `https://futures.mexc.com/exchange/${sym}_USDT`
      return `https://www.mexc.com/exchange/${sym}_USDT`
    case 'bitmart':
      if (market === 'futures') return `https://www.bitmart.com/futures/en-US?symbol=${sym}USDT`
      return `https://www.bitmart.com/trade/en-US?symbol=${sym}_USDT`
    case 'lbank':
      return `https://www.lbank.com/trade/${symLow}_usdt/`
    case 'hyperliquid':
      return `https://app.hyperliquid.xyz/trade/${sym}`
    case 'asterdex':
      return `https://www.asterdex.com/en/trade/pro/futures/${sym}USDT`
    case 'ascendex':
      return `https://ascendex.com/en/cashtrade-spottrading/usdt/${symLow}`
    case 'bybit':
      if (market === 'spot')    return `https://www.bybit.com/trade/spot/${sym}/USDT`
      if (market === 'futures') return `https://www.bybit.com/trade/futures/usdt/${sym}USDT`
      break
    case 'kucoin':
      if (market === 'spot')    return `https://www.kucoin.com/trade/${sym}-USDT`
      if (market === 'futures') return `https://www.kucoin.com/futures/${sym}USDTM`
      break
    case 'binance':
      if (market === 'spot')    return `https://www.binance.com/trade/${sym}_USDT`
      if (market === 'futures') return `https://www.binance.com/futures/${sym}USDT`
      break
    case 'bitget':
      if (market === 'spot')    return `https://www.bitget.com/spot/${sym}USDT`
      if (market === 'futures') return `https://www.bitget.com/futures/${sym}USDT`
      break
    case 'okx':
      if (market === 'spot')    return `https://www.okx.com/trade-spot/${symLow}-usdt`
      if (market === 'futures') return `https://www.okx.com/trade-swap/${symLow}-usdt-swap`
      break
    case 'gmgn':
      return null  // requires contract address — computed separately via getGmgnUrl
  }
  return null
}

// ── Level ─────────────────────────────────────────────────────────────────────

export function getLevelStyle(level: SpreadLevel): string {
  switch (level) {
    case 'VERY_HIGH':
      return 'bg-red-500/15 text-red-400 border-red-500/30'
    case 'High':
    case 'HIGH':
      return 'bg-amber-500/15 text-amber-400 border-amber-500/30'
    default:
      return 'bg-sky-500/15 text-sky-400 border-sky-500/30'
  }
}

export function getLevelLabel(level: SpreadLevel): string {
  switch (level) {
    case 'VERY_HIGH': return 'VERY HIGH'
    case 'High': return 'HIGH'
    default: return level?.toUpperCase() || 'LOW'
  }
}

// ── Token prices extraction ───────────────────────────────────────────────────

export function extractTokenPrices(signal: SpreadSignal): TokenPrice[] {
  const prices: TokenPrice[] = []
  const { tickers_data } = signal

  const types: Array<[keyof TickersData, 'spot' | 'futures' | 'dex']> = [
    ['spot', 'spot'],
    ['futures', 'futures'],
    ['dex', 'dex'],
  ]

  for (const [key, type] of types) {
    const group = tickers_data?.[key] || {}
    for (const [rawKey, ticker] of Object.entries(group)) {
      // Normalise to prefixed key format (e.g. 'gate' -> 'spot_gate', 'gmgn' -> 'dex_gmgn')
      // so it matches liveExchanges keys and signal.buy_from / signal.sell_to
      const exchangeKey = rawKey.includes('_') ? rawKey : `${key}_${rawKey}`
      const bids = ticker.orderbook?.bids || []
      const asks = ticker.orderbook?.asks || []
      const bestBid = bids.length > 0 ? bids[0][0] : null
      const bestAsk = asks.length > 0 ? asks[0][0] : null
      const spreadPct =
        bestBid && bestAsk && bestAsk > 0
          ? ((bestBid - bestAsk) / bestAsk) * 100
          : null

      prices.push({
        exchange: getExchangeLabel(exchangeKey),
        exchangeKey,
        type,
        bestBid,
        bestAsk,
        spread: spreadPct,
        symbol: ticker.symbol || '',
        get_at: ticker.get_at || null,
        chains: ticker.chains || [],
      })
    }
  }

  return prices.sort((a, b) => {
    const order = ['spot', 'futures', 'dex']
    const diff = order.indexOf(a.type) - order.indexOf(b.type)
    if (diff !== 0) return diff
    return a.exchange.localeCompare(b.exchange)
  })
}
