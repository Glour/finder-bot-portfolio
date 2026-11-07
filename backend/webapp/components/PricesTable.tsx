'use client'

import { useCallback, useEffect, useRef, useState } from 'react'
import { AlertTriangle, CheckCircle2, WifiOff, Search, X, Copy, ExternalLink } from 'lucide-react'
import type { TokenPriceRow, ExchangePrice } from '@/lib/prices'
import { formatPrice, formatAge, getExchangeColors } from '@/lib/utils'

// ── Token Detail Panel ────────────────────────────────────────────────────────

interface ChainInfo {
  chain_name: string
  contract_address: string
  deposit_enabled: boolean | null
  withdraw_enabled: boolean | null
  deposit_enabled_changed_at?: string | null
  withdraw_enabled_changed_at?: string | null
}

// Format ISO timestamp as Nd / Nh / Nm / Ns ago
function fmtChangedAt(iso: string | null | undefined): string | null {
  if (!iso) return null
  const diffMs = Date.now() - new Date(iso).getTime()
  if (diffMs < 0) return null
  const diffSec = Math.floor(diffMs / 1000)
  if (diffSec < 60) return `${diffSec}s`
  const diffMin = Math.floor(diffSec / 60)
  if (diffMin < 60) return `${diffMin}m`
  const diffHour = Math.floor(diffMin / 60)
  if (diffHour < 24) return `${diffHour}h`
  const diffDay = Math.floor(diffHour / 24)
  return `${diffDay}d`
}

interface TradingPair {
  source_name: string
  market_type: string
  symbol: string
}

interface TokenDetail {
  global_token_id: string
  symbol: string
  chains: ChainInfo[]
  trading_pairs: TradingPair[]
}

const EXPLORER_URLS: Record<string, string> = {
  ETH:     'https://etherscan.io/token/',
  BSC:     'https://bscscan.com/token/',
  ARB:     'https://arbiscan.io/token/',
  SOL:     'https://solscan.io/token/',
  POLYGON: 'https://polygonscan.com/token/',
  AVAX:    'https://snowtrace.io/token/',
  BASE:    'https://basescan.org/token/',
  OP:      'https://optimistic.etherscan.io/token/',
  TON:     'https://tonscan.org/address/',
  TRX:     'https://tronscan.org/#/token20/',
  SUI:     'https://suiscan.xyz/mainnet/coin/',
  APT:     'https://aptoscan.com/account/',
  BTC:     'https://ordinals.com/',
}

function explorerUrl(chain: string, contract: string): string | null {
  if (!contract || contract.length < 5) return null
  const base = EXPLORER_URLS[chain.toUpperCase()]
  return base ? base + contract : null
}

function tradingUrl(source: string, marketType: string, symbol: string): string | null {
  const mt = marketType.toLowerCase()
  const s = source.toLowerCase()
  switch (s) {
    case 'gate':
      return mt === 'spot'
        ? `https://www.gate.com/trade/${symbol}`
        : `https://www.gate.com/futures/USDT/${symbol}`
    case 'xt':
      return mt === 'spot'
        ? `https://www.xt.com/trade/${symbol.toLowerCase()}`
        : `https://www.xt.com/futures/trade/${symbol.toLowerCase()}`
    case 'mexc':
      return mt === 'spot'
        ? `https://www.mexc.com/exchange/${symbol}`
        : `https://www.mexc.com/futures/${symbol}`
    case 'bybit':
      if (mt === 'spot') {
        const base = symbol.replace('USDT', '')
        return `https://www.bybit.com/trade/spot/${base}/USDT`
      }
      return `https://www.bybit.com/trade/futures/usdt/${symbol}`
    case 'binance':
      if (mt === 'spot') {
        const base = symbol.replace('USDT', '')
        return `https://www.binance.com/trade/${base}_USDT`
      }
      return `https://www.binance.com/futures/${symbol}`
    case 'kucoin':
      return mt === 'spot'
        ? `https://www.kucoin.com/trade/${symbol}`
        : `https://www.kucoin.com/futures/${symbol}`
    case 'okx':
      return mt === 'spot'
        ? `https://www.okx.com/trade-spot/${symbol.toLowerCase()}`
        : `https://www.okx.com/trade-swap/${symbol.toLowerCase()}`
    case 'bitget':
      return mt === 'spot'
        ? `https://www.bitget.com/spot/${symbol}`
        : `https://www.bitget.com/futures/${symbol}`
    case 'bitmart':
      return mt === 'spot'
        ? `https://www.bitmart.com/trade/en-US?symbol=${symbol}`
        : `https://www.bitmart.com/en-US/futures/${symbol.replace('_', '')}`
    case 'lbank':
      return mt === 'spot'
        ? `https://www.lbank.com/trade/${symbol.toLowerCase()}`
        : `https://www.lbank.com/futures/${symbol.toLowerCase()}`
    case 'ascendex':
      if (mt === 'spot') {
        const parts = symbol.includes('/') ? symbol.split('/') : symbol.includes('_') ? symbol.split('_') : null
        if (!parts) return null
        return `https://ascendex.com/en/cashtrade-spottrading/${parts[1].toLowerCase()}/${parts[0].toLowerCase()}`
      }
      return `https://ascendex.com/en/futures-perpetualcontract-trading/${symbol.toLowerCase()}`
    case 'asterdex':
      return mt === 'spot'
        ? `https://www.asterdex.com/en/trade/pro/spot/${symbol.replace('_', '')}`
        : `https://www.asterdex.com/en/trade/pro/futures/${symbol.replace('_', '')}`
    case 'hyperliquid':
      return mt === 'spot'
        ? `https://app.hyperliquid.xyz/trade/${symbol}`
        : `https://app.hyperliquid.xyz/trade/${symbol}`
    default:
      return null
  }
}

const EXCHANGE_DISPLAY: Record<string, string> = {
  gate: 'Gate.io', xt: 'XT.com', mexc: 'MEXC', bybit: 'Bybit', binance: 'Binance',
  kucoin: 'KuCoin', okx: 'OKX', bitget: 'Bitget', bitmart: 'BitMart',
  lbank: 'LBank', ascendex: 'AscendEX', asterdex: 'AsterDEX', hyperliquid: 'HyperLiquid',
}

function TokenDetailPanel({
  tokenId,
  symbol,
  onClose,
}: {
  tokenId: string
  symbol: string
  onClose: () => void
}) {
  const [detail, setDetail] = useState<TokenDetail | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [copied, setCopied] = useState<string | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    fetch(`/api/token?id=${encodeURIComponent(tokenId)}`, { signal: controller.signal })
      .then((r) => {
        if (!r.ok) throw new Error('Failed to load token details')
        return r.json()
      })
      .then((j) => { setDetail(j.data); setLoading(false) })
      .catch((e) => {
        if (!controller.signal.aborted) { setError(String(e)); setLoading(false) }
      })
    return () => controller.abort()
  }, [tokenId])

  const handleCopy = (text: string) => {
    navigator.clipboard.writeText(text).catch(() => {})
    setCopied(text)
    setTimeout(() => setCopied(null), 1500)
  }

  return (
    <div className="flex flex-col h-full animate-[slideIn_0.2s_ease-out]">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-[#1e2d45] bg-[#0d1321] flex-shrink-0">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-lg font-bold text-white font-num">{symbol}</span>
            <span className="text-slate-500 text-sm">/USDT</span>
          </div>
          <p className="text-[11px] text-slate-600 font-num mt-0.5 break-all leading-snug">{tokenId}</p>
        </div>
        <button
          onClick={onClose}
          className="p-1.5 rounded hover:bg-white/10 text-slate-400 hover:text-white transition-colors flex-shrink-0"
        >
          <X size={16} />
        </button>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto">
        {loading && (
          <div className="flex items-center justify-center py-16 text-slate-500 text-sm">Loading…</div>
        )}
        {error && (
          <div className="flex items-center justify-center py-16 text-red-400 text-sm">{error}</div>
        )}
        {!loading && !error && detail && detail.chains.length === 0 && detail.trading_pairs.length === 0 && (
          <div className="flex flex-col items-center justify-center py-16 text-slate-500">
            <AlertTriangle size={24} className="mb-2" />
            <p className="text-sm">No chain / contract data</p>
          </div>
        )}
        {!loading && !error && detail && detail.trading_pairs.length > 0 && (
          <div className="px-4 pt-4 pb-2">
            <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">
              Trading Pairs ({detail.trading_pairs.length})
            </h3>
            <div className="flex flex-col gap-1">
              {detail.trading_pairs.map((p, i) => {
                const url = tradingUrl(p.source_name, p.market_type, p.symbol)
                const displayName = EXCHANGE_DISPLAY[p.source_name] ?? p.source_name
                const mtLabel = p.market_type === 'SPOT' ? 'S' : 'F'
                const mtColor = p.market_type === 'SPOT' ? 'text-emerald-400 bg-emerald-900/30 border-emerald-700/40' : 'text-purple-300 bg-purple-900/30 border-purple-700/40'
                return (
                  <div key={i} className="flex items-center justify-between bg-[#0d1321] border border-[#1e2d45] rounded-lg px-3 py-2">
                    <div className="flex items-center gap-2">
                      <span className={`px-1.5 py-0.5 rounded text-[10px] font-bold border ${mtColor}`}>{mtLabel}</span>
                      <span className="text-slate-200 text-xs font-medium">{displayName}</span>
                      <span className="text-slate-500 text-[11px] font-num">{p.symbol}</span>
                    </div>
                    {url ? (
                      <a
                        href={url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center gap-1 text-sky-400 hover:text-sky-300 text-xs transition-colors"
                      >
                        Trade <ExternalLink size={11} />
                      </a>
                    ) : (
                      <span className="text-slate-600 text-xs">—</span>
                    )}
                  </div>
                )
              })}
            </div>
          </div>
        )}
        {!loading && !error && detail && detail.chains.length > 0 && (
          <div className="px-4 pt-4 pb-4 flex flex-col gap-2">
            <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-1">
              Chains ({detail.chains.length})
            </h3>
            {detail.chains.map((c, i) => {
              const url = explorerUrl(c.chain_name, c.contract_address)
              return (
                <div
                  key={i}
                  className="bg-[#0d1321] border border-[#1e2d45] rounded-lg px-3 py-2.5"
                >
                  <div className="flex items-center justify-between mb-1.5">
                    <span className="px-2 py-0.5 rounded text-[11px] font-bold bg-sky-900/40 text-sky-300 border border-sky-700/40 uppercase tracking-wide">
                      {c.chain_name}
                    </span>
                    <div className="flex items-center gap-2 text-[10px] font-num">
                      <span className={c.deposit_enabled ? 'text-emerald-400' : 'text-slate-600'}>
                        DEP {c.deposit_enabled ? '✓' : '✗'}
                        {fmtChangedAt(c.deposit_enabled_changed_at) && (
                          <span className="text-slate-500 ml-0.5 text-[9px]">{fmtChangedAt(c.deposit_enabled_changed_at)}</span>
                        )}
                      </span>
                      <span className={c.withdraw_enabled ? 'text-emerald-400' : 'text-slate-600'}>
                        WD {c.withdraw_enabled ? '✓' : '✗'}
                        {fmtChangedAt(c.withdraw_enabled_changed_at) && (
                          <span className="text-slate-500 ml-0.5 text-[9px]">{fmtChangedAt(c.withdraw_enabled_changed_at)}</span>
                        )}
                      </span>
                    </div>
                  </div>

                  {c.contract_address ? (
                    <div className="flex items-center gap-1.5">
                      <span className="text-slate-300 text-xs font-num break-all leading-snug flex-1">
                        {c.contract_address}
                      </span>
                      <div className="flex items-center gap-1 shrink-0">
                        <button
                          onClick={() => handleCopy(c.contract_address)}
                          className="text-slate-600 hover:text-slate-300 transition-colors"
                          title="Copy contract"
                        >
                          {copied === c.contract_address
                            ? <CheckCircle2 size={12} className="text-emerald-400" />
                            : <Copy size={12} />}
                        </button>
                        {url && (
                          <a
                            href={url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-slate-600 hover:text-sky-400 transition-colors"
                            title="Open explorer"
                          >
                            <ExternalLink size={12} />
                          </a>
                        )}
                      </div>
                    </div>
                  ) : (
                    <span className="text-slate-600 text-xs italic">Native token (no contract)</span>
                  )}
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}

// ── Exchange columns config ──────────────────────────────────────────────────

const EXCHANGE_COLUMNS: Array<{ key: string; label: string; short: string }> = [
  { key: 'spot_gate',        label: 'Gate Spot',    short: 'Gate S' },
  { key: 'futures_gate',     label: 'Gate Fut',     short: 'Gate F' },
  { key: 'spot_xt',          label: 'XT Spot',      short: 'XT S' },
  { key: 'futures_xt',       label: 'XT Fut',       short: 'XT F' },
  { key: 'spot_mexc',        label: 'MEXC Spot',    short: 'MEXC S' },
  { key: 'futures_mexc',     label: 'MEXC Fut',     short: 'MEXC F' },
  { key: 'spot_bitmart',     label: 'BitMart S',    short: 'BM S' },
  { key: 'futures_bitmart',  label: 'BitMart F',    short: 'BM F' },
  { key: 'spot_lbank',       label: 'LBank Spot',   short: 'LB S' },
  { key: 'futures_lbank',    label: 'LBank Fut',    short: 'LB F' },
  { key: 'spot_hyperliquid', label: 'HyperLiq',     short: 'HL' },
  { key: 'spot_asterdex',    label: 'AsterDEX S',   short: 'AstS' },
  { key: 'futures_asterdex', label: 'AsterDEX',     short: 'Ast' },
  { key: 'spot_ascendex',    label: 'AscendEX S',   short: 'Asc S' },
  { key: 'futures_ascendex', label: 'AscendEX F',   short: 'Asc F' },
  { key: 'spot_bybit',       label: 'Bybit Spot',   short: 'BB S' },
  { key: 'futures_bybit',    label: 'Bybit Fut',    short: 'BB F' },
  { key: 'spot_kucoin',      label: 'KuCoin Spot',  short: 'KC S' },
  { key: 'futures_kucoin',   label: 'KuCoin Fut',   short: 'KC F' },
  { key: 'spot_binance',     label: 'Binance Spot', short: 'BN S' },
  { key: 'futures_binance',  label: 'Binance Fut',  short: 'BN F' },
  { key: 'spot_bitget',      label: 'Bitget Spot',  short: 'BG S' },
  { key: 'futures_bitget',   label: 'Bitget Fut',   short: 'BG F' },
  { key: 'spot_okx',         label: 'OKX Spot',     short: 'OKX S' },
  { key: 'futures_okx',      label: 'OKX Fut',      short: 'OKX F' },
  { key: 'dex_gmgn',         label: 'GMGN',          short: 'GMGN' },
]

// ── Cell component ────────────────────────────────────────────────────────────

function PriceCell({ price, colKey, now }: { price: ExchangePrice | undefined; colKey: string; now: number }) {
  if (!price) {
    // Token not registered on this exchange
    return (
      <td className="px-1 py-1.5 text-center">
        <span className="text-slate-700 text-xs">—</span>
      </td>
    )
  }

  if (price.bid === null) {
    // Token is registered on exchange but no price data yet (waiting for collector)
    return (
      <td className="px-1 py-1.5 text-center">
        <span className="text-slate-600 text-xs" title="Listed on exchange, awaiting price data">·</span>
      </td>
    )
  }

  const colors = getExchangeColors(colKey)
  const stale30 = price.get_at ? now - price.get_at > 30_000 : true
  const stale60 = price.get_at ? now - price.get_at > 60_000 : true
  const age = price.get_at ? Math.floor((now - price.get_at) / 1000) : null

  return (
    <td className="px-1 py-1.5">
      <div className="flex flex-col items-end gap-0.5">
        <span className={`font-num text-xs font-medium ${stale60 ? 'text-red-400' : stale30 ? 'text-amber-400' : colors.text}`}>
          {formatPrice(price.bid)}
        </span>
        {age !== null && (
          <span className={`text-[10px] font-num ${stale60 ? 'text-red-500' : stale30 ? 'text-amber-500/70' : 'text-slate-600'}`}>
            {age < 10 ? `${age}s` : stale60 ? `${age}s⚠` : `${age}s`}
          </span>
        )}
      </div>
    </td>
  )
}

// ── Exchange header cell ───────────────────────────────────────────────────────

function ExchangeHeader({ col }: { col: typeof EXCHANGE_COLUMNS[0] }) {
  const colors = getExchangeColors(col.key)
  return (
    <th className={`px-1 py-2 text-center text-[10px] font-semibold uppercase tracking-wide whitespace-nowrap ${colors.text}`}>
      {col.short}
    </th>
  )
}

// ── Main component ────────────────────────────────────────────────────────────

export default function PricesTable() {
  const [rows, setRows] = useState<TokenPriceRow[]>([])
  const [connected, setConnected] = useState(false)
  const [lastUpdate, setLastUpdate] = useState<number | null>(null)
  const [search, setSearch] = useState('')
  const [now, setNow] = useState(() => Date.now())
  const [filterExchanges, setFilterExchanges] = useState<Set<string>>(new Set())
  const [selectedToken, setSelectedToken] = useState<{ id: string; symbol: string } | null>(null)
  const [contractResult, setContractResult] = useState<{ query: string; ids: Set<string> } | null>(null)
  const esRef = useRef<EventSource | null>(null)

  // Detect contract-like search: starts with 0x or is 30+ hex chars
  const isContractSearch = /^(0x)?[0-9a-fA-F]{20,}$/.test(search.replace(/\s/g, ''))
  const contractMatchIds = contractResult?.query === search ? contractResult.ids : null
  const contractSearching = isContractSearch && contractMatchIds === null

  useEffect(() => {
    if (!isContractSearch || search.length < 8) return
    const controller = new AbortController()
    const t = setTimeout(() => {
      fetch(`/api/search?q=${encodeURIComponent(search.trim())}`, { signal: controller.signal })
        .then((r) => {
          if (!r.ok) throw new Error('Search failed')
          return r.json()
        })
        .then((d) => {
          setContractResult({ query: search, ids: new Set(d.ids || []) })
        })
        .catch(() => {
          if (!controller.signal.aborted) setContractResult({ query: search, ids: new Set() })
        })
    }, 300)
    return () => { clearTimeout(t); controller.abort() }
  }, [search, isContractSearch])

  const toggleFilterExchange = (key: string) => {
    setFilterExchanges((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(t)
  }, [])

  const connect = useCallback(() => {
    if (esRef.current) esRef.current.close()
    const es = new EventSource('/api/prices/stream')
    esRef.current = es
    es.onopen = () => setConnected(true)
    es.onmessage = (e) => {
      try {
        const payload = JSON.parse(e.data)
        if (payload.type === 'prices') {
          setRows(payload.data)
          setLastUpdate(payload.ts)
          setConnected(true)
        }
      } catch {}
    }
    es.onerror = () => {
      setConnected(false)
    }
  }, [])

  useEffect(() => {
    connect()
    return () => esRef.current?.close()
  }, [connect])

  const filtered = rows.filter((r) => {
    if (search) {
      if (isContractSearch) {
        // Contract search: filter by matching global_token_ids
        if (contractMatchIds !== null && !contractMatchIds.has(r.global_token_id)) return false
        if (contractMatchIds === null) return false  // still loading
      } else {
        // Symbol search
        if (!r.symbol.toLowerCase().includes(search.toLowerCase())) return false
      }
    }
    if (filterExchanges.size > 0) {
      // Show token if it's registered on any of the selected exchanges (even without price)
      const isListed = [...filterExchanges].some((key) => r.exchanges[key] !== undefined)
      if (!isListed) return false
    }
    return true
  })

  const activeCols = EXCHANGE_COLUMNS.filter((col) =>
    // Show column if any token is registered on this exchange (with or without price)
    rows.some((r) => r.exchanges[col.key] !== undefined)
  )

  const totalWithData = (colKey: string) =>
    // Count all registered tokens for this exchange (including those without prices)
    rows.filter((r) => r.exchanges[colKey] !== undefined).length

  const staleCount = (colKey: string) =>
    rows.filter((r) => {
      const p = r.exchanges[colKey]
      return p?.bid != null && (p?.get_at ? now - p.get_at > 60_000 : true)
    }).length

  const noDataCount = (colKey: string) =>
    rows.filter((r) => r.exchanges[colKey] !== undefined && r.exchanges[colKey].bid === null).length

  return (
    <div className="flex h-full overflow-hidden">
      {/* ── Main table area ── */}
      <div className={`flex flex-col overflow-hidden transition-all duration-200 ${selectedToken ? 'flex-1' : 'w-full'}`}>
        {/* Sub-header */}
        <div className="flex items-center justify-between px-4 py-2 border-b border-[#1e2d45] bg-[#0a0f1c] flex-shrink-0">
          <div className="flex items-center gap-3">
            <div className="relative">
              <Search size={13} className="absolute left-2 top-1/2 -translate-y-1/2 text-slate-500" />
              <input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Symbol or contract…"
                className={`pl-7 pr-3 py-1.5 text-xs bg-[#0d1321] border rounded text-slate-300
                  focus:outline-none w-48 font-num ${
                    isContractSearch
                      ? 'border-purple-500/60 focus:border-purple-400'
                      : 'border-[#1e2d45] focus:border-sky-500/50'
                  }`}
              />
              {isContractSearch && (
                <span className="absolute right-2 top-1/2 -translate-y-1/2 text-xs text-purple-400">
                  {contractSearching ? '…' : '🔗'}
                </span>
              )}
            </div>
            <span className="text-xs text-slate-500">
              {filtered.length} token{filtered.length !== 1 ? 's' : ''}
              {filterExchanges.size > 0 && (
                <span className="ml-1.5 text-sky-400">
                  · {filterExchanges.size} exchange{filterExchanges.size > 1 ? 's' : ''} selected
                </span>
              )}
            </span>
          </div>
          {lastUpdate && (
            <span className="text-xs text-slate-500 font-num">
              Updated {formatAge(lastUpdate)} ago
            </span>
          )}
        </div>

        {/* Exchange coverage bar */}
        <div className="flex flex-wrap items-center gap-1.5 px-4 py-2 border-b border-[#1e2d45] bg-[#060a10]">
          <span className="text-[10px] text-slate-600 uppercase tracking-wider mr-1 shrink-0">Coverage:</span>
          {activeCols.map((col) => {
            const count = totalWithData(col.key)
            const stale = staleCount(col.key)
            const noData = noDataCount(col.key)
            const colors = getExchangeColors(col.key)
            const hasIssue = stale > 0
            const isSelected = filterExchanges.has(col.key)
            return (
              <button
                key={col.key}
                onClick={() => toggleFilterExchange(col.key)}
                title={isSelected ? `Remove ${col.label} filter` : `Show only tokens with ${col.label}`}
                className={`flex items-center gap-1 px-2 py-0.5 rounded border text-[10px] shrink-0 cursor-pointer transition-all
                  ${isSelected
                    ? `${colors.bg} border-sky-400 ring-1 ring-sky-400/50`
                    : `${colors.bg} ${hasIssue ? 'border-amber-500/40' : colors.border} opacity-80 hover:opacity-100`
                  }`}
              >
                <span className={colors.text + ' font-medium'}>{col.short}</span>
                <span className="text-slate-500">{count}</span>
                {hasIssue && <span className="text-amber-400">⚠{stale}</span>}
                {noData > 0 && <span className="text-slate-600">·{noData}</span>}
                {!hasIssue && noData === 0 && count > 0 && <CheckCircle2 size={9} className="text-emerald-500" />}
              </button>
            )
          })}
          {filterExchanges.size > 0 && (
            <button
              onClick={() => setFilterExchanges(new Set())}
              className="flex items-center gap-1 px-2 py-0.5 rounded border border-red-500/40 text-[10px] text-red-400 hover:bg-red-500/10 transition-colors shrink-0 ml-1"
              title="Clear exchange filter"
            >
              ✕ clear
            </button>
          )}
          {activeCols.length === 0 && (
            <span className="text-xs text-slate-600">No data</span>
          )}
        </div>

        {/* Table */}
        <div className="flex-1 overflow-auto">
          <table className="w-full text-xs border-collapse">
            <thead className="sticky top-0 z-10 bg-[#0d1321]">
              <tr className="border-b border-[#1e2d45]">
                <th className="text-left pl-4 pr-2 py-2 text-slate-400 font-semibold uppercase tracking-wider text-[10px] sticky left-0 bg-[#0d1321]">
                  Token
                </th>
                {activeCols.map((col) => (
                  <ExchangeHeader key={col.key} col={col} />
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-[#1e2d45]/30">
              {filtered.length === 0 && (
                <tr>
                  <td colSpan={activeCols.length + 1} className="text-center py-16 text-slate-600">
                    {rows.length === 0 && connected ? 'Loading…' : !connected ? (
                      <span className="flex items-center justify-center gap-2">
                        <WifiOff size={14} /> No connection
                      </span>
                    ) : (
                      <span>No data</span>
                    )}
                  </td>
                </tr>
              )}
              {filtered.map((row) => {
                const exchangeCount = activeCols.filter((col) => row.exchanges[col.key]?.bid != null).length
                const hasStale = activeCols.some((col) => {
                  const p = row.exchanges[col.key]
                  return p?.bid != null && (p?.get_at ? now - p.get_at > 60_000 : true)
                })
                const isSelected = selectedToken?.id === row.global_token_id

                return (
                  <tr
                    key={row.global_token_id}
                    className={`transition-colors duration-75 cursor-pointer ${isSelected ? 'bg-sky-500/10' : 'hover:bg-white/[0.02]'}`}
                    onClick={() => setSelectedToken(isSelected ? null : { id: row.global_token_id, symbol: row.symbol })}
                  >
                    <td className={`pl-4 pr-3 py-1.5 sticky left-0 ${isSelected ? 'bg-sky-500/10' : 'bg-[#0a0e17]'}`}>
                      <div className="flex items-center gap-2 group">
                        <span className={`font-semibold font-num text-sm ${isSelected ? 'text-sky-300' : 'text-white group-hover:text-sky-300'} transition-colors`}>
                          {row.symbol}
                        </span>
                        <span className="text-[10px] text-slate-600 font-num">
                          {exchangeCount}/{activeCols.length}
                        </span>
                        {hasStale && <AlertTriangle size={10} className="text-amber-400" />}
                      </div>
                    </td>
                    {activeCols.map((col) => (
                      <PriceCell key={col.key} price={row.exchanges[col.key]} colKey={col.key} now={now} />
                    ))}
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-4 py-2 border-t border-[#1e2d45] bg-[#0d1321] text-xs text-slate-500 flex-shrink-0">
          <span>
            <span className="text-emerald-400 mr-1">●</span>Fresh &lt;30s
            <span className="text-amber-400 ml-3 mr-1">●</span>30-60s
            <span className="text-red-400 ml-3 mr-1">●</span>Stale &gt;60s
            <span className="text-slate-600 ml-3 mr-1">·</span>Listed, no price yet
            <span className="text-slate-700 ml-3 mr-1">—</span>Not on exchange
            <span className="text-slate-600 ml-4">· Click token for chain info</span>
          </span>
          <span className="font-num">Auto-refresh 3s</span>
        </div>
      </div>

      {/* ── Side panel ── */}
      {selectedToken && (
        <div className="w-[420px] flex-shrink-0 border-l border-[#1e2d45] bg-[#0a0e17] overflow-hidden flex flex-col">
          <TokenDetailPanel
            key={selectedToken.id}
            tokenId={selectedToken.id}
            symbol={selectedToken.symbol}
            onClose={() => setSelectedToken(null)}
          />
        </div>
      )}
    </div>
  )
}
