'use client'

import { useCallback, useEffect, useRef, useState } from 'react'
import {
  RefreshCw,
  TrendingUp,
  Wifi,
  WifiOff,
  ChevronUp,
  ChevronDown,
  Filter,
  X,
  BarChart2,
  Table2,
  VolumeX,
} from 'lucide-react'
import type { SpreadRow } from '@/lib/types'
import PricesTable from '@/components/PricesTable'
import MuteTable from '@/components/MuteTable'
import {
  formatPrice,
  formatSpread,
  formatProfit,
  formatVolume,
  formatAge,
  isStale,
  getLevelLabel,
  getExchangeName,
} from '@/lib/utils'
import ExchangeBadge from '@/components/ExchangeBadge'
import LevelBadge from '@/components/LevelBadge'
import PricesPanel from '@/components/PricesPanel'

// ── Types ──────────────────────────────────────────────────────────────────────

type SortKey = 'spread_percent' | 'profit' | 'volume' | 'analyzed_at' | 'symbol' | 'buy_price'
type SortDir = 'asc' | 'desc'

interface FilterState {
  exchange: string
  level: string
  minSpread: string
}

// ── Main Dashboard ─────────────────────────────────────────────────────────────

export default function Dashboard() {
  const [activeTab, setActiveTab] = useState<'spreads' | 'prices' | 'mutes'>('spreads')
  const [rows, setRows] = useState<SpreadRow[]>([])
  const [selected, setSelected] = useState<SpreadRow | null>(null)
  const [connected, setConnected] = useState(false)
  const [lastUpdate, setLastUpdate] = useState<number | null>(null)
  const [sortKey, setSortKey] = useState<SortKey>('spread_percent')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [filters, setFilters] = useState<FilterState>({ exchange: '', level: '', minSpread: '' })
  const [freshOnly, setFreshOnly] = useState(false)
  const [showFilters, setShowFilters] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const esRef = useRef<EventSource | null>(null)
  const prevRowsRef = useRef<Map<string, SpreadRow>>(new Map())

  // ── SSE connection ─────────────────────────────────────────────────────────

  const connect = useCallback(() => {
    if (esRef.current) {
      esRef.current.close()
    }
    const es = new EventSource('/api/stream')
    esRef.current = es

    es.onopen = () => {
      setConnected(true)
      setError(null)
    }

    es.onmessage = (e) => {
      try {
        const payload = JSON.parse(e.data)
        if (payload.type === 'spreads') {
          const newMap = new Map<string, SpreadRow>()
          for (const row of payload.data as SpreadRow[]) {
            newMap.set(row.key, row)
          }
          prevRowsRef.current = newMap
          setRows(payload.data)
          setLastUpdate(payload.ts)
          setConnected(true)
          setError(null)
          // Keep selected in sync with latest signal data
          setSelected((prev) => prev ? (newMap.get(prev.key) ?? prev) : null)
        } else if (payload.type === 'error') {
          setError(payload.message)
        }
      } catch {
        // ignore parse errors
      }
    }

    es.onerror = () => {
      setConnected(false)
      setError('Connection lost — reconnecting…')
    }
  }, [])

  useEffect(() => {
    connect()
    return () => esRef.current?.close()
  }, [connect])

  // ── Sort & Filter ──────────────────────────────────────────────────────────

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortKey(key)
      setSortDir('desc')
    }
  }

  const filteredRows = rows.filter((r) => {
    const s = r.signal
    if (filters.exchange) {
      const ex = filters.exchange.toLowerCase()
      const buyMatch = s.buy_from.toLowerCase().includes(ex)
      const sellMatch = s.sell_to.toLowerCase().includes(ex)
      if (!buyMatch && !sellMatch) return false
    }
    if (filters.level) {
      if (s.level !== filters.level) return false
    }
    if (filters.minSpread) {
      const min = parseFloat(filters.minSpread)
      if (!isNaN(min) && s.spread_percent < min) return false
    }
    if (freshOnly && isStale(s.analyzed_at, 10 * 60_000)) return false
    return true
  })

  const sortedRows = [...filteredRows].sort((a, b) => {
    const mult = sortDir === 'asc' ? 1 : -1
    // Spread: use live_spread when available (matches displayed value)
    if (sortKey === 'spread_percent') {
      const av = a.signal.live_spread ?? a.signal.spread_percent
      const bv = b.signal.live_spread ?? b.signal.spread_percent
      return (av - bv) * mult
    }
    // Buy price: use live_buy_price when available (matches displayed value)
    if (sortKey === 'buy_price') {
      const av = a.signal.live_buy_price ?? a.signal.buy_price
      const bv = b.signal.live_buy_price ?? b.signal.buy_price
      return (av - bv) * mult
    }
    const av = a.signal[sortKey as keyof typeof a.signal] as number | string
    const bv = b.signal[sortKey as keyof typeof b.signal] as number | string
    if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * mult
    return String(av).localeCompare(String(bv)) * mult
  })

  // ── Unique exchanges & levels for filters ──────────────────────────────────

  const allExchanges = Array.from(new Set(
    rows.flatMap((r) => [
      getExchangeName(r.signal.buy_from),
      getExchangeName(r.signal.sell_to),
    ])
  )).sort()

  const allLevels = Array.from(new Set(rows.map((r) => r.signal.level))).sort()

  // ── Derived stats ──────────────────────────────────────────────────────────

  const maxSpread = rows.reduce((m, r) => Math.max(m, r.signal.spread_percent), 0)
  const totalProfit = rows.reduce((s, r) => s + r.signal.profit, 0)

  // ── Render ─────────────────────────────────────────────────────────────────

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-[#0a0e17]">
      {/* ── Header ── */}
      <header className="flex items-center justify-between px-4 py-2.5 border-b border-[#1e2d45] bg-[#0d1321] flex-shrink-0">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <TrendingUp size={18} className="text-sky-400" />
            <span className="font-bold text-white tracking-tight">Finder</span>
            <span className="text-slate-500 text-sm hidden sm:inline">Arbitrage Monitor</span>
          </div>
          {/* Tabs */}
          <div className="flex items-center bg-[#060a10] border border-[#1e2d45] rounded p-0.5 gap-0.5">
            <button
              onClick={() => setActiveTab('spreads')}
              className={`flex items-center gap-1.5 px-3 py-1 rounded text-xs font-medium transition-colors
                ${activeTab === 'spreads'
                  ? 'bg-sky-500/20 text-sky-400 border border-sky-500/30'
                  : 'text-slate-500 hover:text-slate-300'}`}
            >
              <BarChart2 size={12} />
              Spreads
            </button>
            <button
              onClick={() => { setActiveTab('prices'); setSelected(null) }}
              className={`flex items-center gap-1.5 px-3 py-1 rounded text-xs font-medium transition-colors
                ${activeTab === 'prices'
                  ? 'bg-sky-500/20 text-sky-400 border border-sky-500/30'
                  : 'text-slate-500 hover:text-slate-300'}`}
            >
              <Table2 size={12} />
              Prices
            </button>
            <button
              onClick={() => { setActiveTab('mutes'); setSelected(null) }}
              className={`flex items-center gap-1.5 px-3 py-1 rounded text-xs font-medium transition-colors
                ${activeTab === 'mutes'
                  ? 'bg-red-500/20 text-red-400 border border-red-500/30'
                  : 'text-slate-500 hover:text-slate-300'}`}
            >
              <VolumeX size={12} />
              Mutes
            </button>
          </div>
          <div className="h-4 w-px bg-[#1e2d45]" />
          <div className="flex items-center gap-1.5">
            <span
              className={`inline-block w-2 h-2 rounded-full pulse-dot ${
                connected ? 'bg-emerald-400' : 'bg-red-400'
              }`}
            />
            {connected ? (
              <Wifi size={13} className="text-emerald-400" />
            ) : (
              <WifiOff size={13} className="text-red-400" />
            )}
            <span className={`text-xs font-num ${connected ? 'text-emerald-400' : 'text-red-400'}`}>
              {connected ? 'LIVE' : 'OFFLINE'}
            </span>
          </div>
          {lastUpdate && (
            <span className="text-xs text-slate-500 font-num">
              Updated {formatAge(lastUpdate)} ago
            </span>
          )}
        </div>

        {/* Stats — spreads tab only */}
        <div className={`flex items-center gap-4 ${activeTab !== 'spreads' ? 'hidden' : ''}`}>
          <StatPill label="Signals" value={sortedRows.length} total={rows.length} />
          <StatPill label="Best" value={`${maxSpread.toFixed(2)}%`} />
          <StatPill label="Total profit" value={formatProfit(totalProfit)} />
          <button
            onClick={() => setShowFilters((v) => !v)}
            className={`flex items-center gap-1.5 text-xs px-2.5 py-1.5 rounded border transition-colors
              ${showFilters
                ? 'bg-sky-500/20 text-sky-400 border-sky-500/40'
                : 'bg-white/5 text-slate-400 border-[#1e2d45] hover:border-[#2e4060]'
              }`}
          >
            <Filter size={12} />
            Filters
            {(filters.exchange || filters.level || filters.minSpread || freshOnly) && (
              <span className="w-1.5 h-1.5 rounded-full bg-sky-400" />
            )}
          </button>
        </div>
      </header>

      {/* ── Filter bar ── */}
      {showFilters && activeTab === 'spreads' && (
        <div className="flex items-center gap-3 px-4 py-2 border-b border-[#1e2d45] bg-[#0a0f1c] flex-shrink-0 animate-[fadeIn_0.15s_ease-out]">
          <select
            value={filters.exchange}
            onChange={(e) => setFilters((f) => ({ ...f, exchange: e.target.value }))}
            className="bg-[#0d1321] border border-[#1e2d45] text-slate-300 text-xs rounded px-2 py-1.5 focus:outline-none focus:border-sky-500/50"
          >
            <option value="">All exchanges</option>
            {allExchanges.map((ex) => (
              <option key={ex} value={ex}>{ex}</option>
            ))}
          </select>

          <select
            value={filters.level}
            onChange={(e) => setFilters((f) => ({ ...f, level: e.target.value }))}
            className="bg-[#0d1321] border border-[#1e2d45] text-slate-300 text-xs rounded px-2 py-1.5 focus:outline-none focus:border-sky-500/50"
          >
            <option value="">All levels</option>
            {allLevels.map((lv) => (
              <option key={lv} value={lv}>{getLevelLabel(lv)}</option>
            ))}
          </select>

          <div className="flex items-center gap-1.5">
            <span className="text-xs text-slate-500">Min spread</span>
            <input
              type="number"
              value={filters.minSpread}
              onChange={(e) => setFilters((f) => ({ ...f, minSpread: e.target.value }))}
              placeholder="0"
              className="w-20 bg-[#0d1321] border border-[#1e2d45] text-slate-300 text-xs rounded px-2 py-1.5
                focus:outline-none focus:border-sky-500/50 font-num"
            />
            <span className="text-xs text-slate-500">%</span>
          </div>

          <button
            onClick={() => setFreshOnly((v) => !v)}
            title="Hide signals not updated in > 10 min (stale price data)"
            className={`flex items-center gap-1.5 text-xs px-2.5 py-1.5 rounded border transition-colors
              ${freshOnly
                ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/40'
                : 'bg-white/5 text-slate-400 border-[#1e2d45] hover:border-[#2e4060]'
              }`}
          >
            <span className={`w-1.5 h-1.5 rounded-full ${freshOnly ? 'bg-emerald-400' : 'bg-slate-500'}`} />
            Fresh only (&lt;10m)
          </button>

          {(filters.exchange || filters.level || filters.minSpread || freshOnly) && (
            <button
              onClick={() => { setFilters({ exchange: '', level: '', minSpread: '' }); setFreshOnly(false) }}
              className="flex items-center gap-1 text-xs text-slate-400 hover:text-red-400 transition-colors"
            >
              <X size={12} />
              Clear
            </button>
          )}
        </div>
      )}

      {/* ── Error banner ── */}
      {error && (
        <div className="flex items-center gap-2 px-4 py-2 bg-red-500/10 border-b border-red-500/20 text-red-400 text-xs flex-shrink-0">
          <WifiOff size={12} />
          {error}
        </div>
      )}

      {/* ── Main content ── */}
      {activeTab === 'prices' && (
        <div className="flex-1 overflow-hidden">
          <PricesTable />
        </div>
      )}
      {activeTab === 'mutes' && (
        <div className="flex-1 overflow-hidden">
          <MuteTable />
        </div>
      )}
      <div className={`flex flex-1 overflow-hidden ${activeTab !== 'spreads' ? 'hidden' : ''}`}>
        {/* ── Spreads table ── */}
        <div className={`flex flex-col overflow-hidden transition-all duration-200 ${selected ? 'flex-1' : 'w-full'}`}>
          <div className="overflow-auto flex-1">
            <table className="w-full text-sm border-collapse">
              <thead className="sticky top-0 z-10">
                <tr className="bg-[#0d1321] border-b border-[#1e2d45]">
                  <Th className="w-8 pl-4">#</Th>
                  <SortTh label="Symbol" sortKey="symbol" current={sortKey} dir={sortDir} onSort={handleSort} />
                  <Th>Buy from</Th>
                  <Th>Sell to</Th>
                  <SortTh label="Buy price" sortKey="buy_price" current={sortKey} dir={sortDir} onSort={handleSort} className="hidden lg:table-cell" />
                  <SortTh label="Spread" sortKey="spread_percent" current={sortKey} dir={sortDir} onSort={handleSort} />
                  <SortTh label="Profit" sortKey="profit" current={sortKey} dir={sortDir} onSort={handleSort} />
                  <SortTh label="Volume" sortKey="volume" current={sortKey} dir={sortDir} onSort={handleSort} className="hidden xl:table-cell" />
                  <Th>Level</Th>
                  <SortTh label="Age" sortKey="analyzed_at" current={sortKey} dir={sortDir} onSort={handleSort} className="hidden md:table-cell" />
                </tr>
              </thead>
              <tbody className="divide-y divide-[#1e2d45]/50">
                {sortedRows.length === 0 && (
                  <tr>
                    <td colSpan={10} className="text-center py-16 text-slate-500">
                      {rows.length === 0 && connected ? (
                        <div className="flex flex-col items-center gap-2">
                          <RefreshCw size={20} className="animate-spin opacity-50" />
                          <span className="text-sm">Loading spreads…</span>
                        </div>
                      ) : (
                        <span className="text-sm">No data</span>
                      )}
                    </td>
                  </tr>
                )}
                {sortedRows.map((row, idx) => {
                  const s = row.signal
                  const isSelected = selected?.key === row.key
                  return (
                    <tr
                      key={row.key}
                      className={`spread-row ${isSelected ? 'selected' : ''}`}
                      onClick={() => setSelected(isSelected ? null : row)}
                    >
                      {/* # */}
                      <td className="pl-4 pr-2 py-2.5 text-slate-600 text-xs font-num w-8">
                        {idx + 1}
                      </td>

                      {/* Symbol */}
                      <td className="px-2 py-2.5">
                        <span className="font-semibold text-white font-num">{s.symbol}</span>
                        <span className="text-slate-500 text-xs">/USDT</span>
                      </td>

                      {/* Buy from */}
                      <td className="px-2 py-2.5">
                        <ExchangeBadge sourceKey={s.buy_from} />
                      </td>

                      {/* Sell to */}
                      <td className="px-2 py-2.5">
                        <ExchangeBadge sourceKey={s.sell_to} />
                      </td>

                      {/* Buy price */}
                      <td className="px-2 py-2.5 font-num text-slate-300 text-right hidden lg:table-cell">
                        <span className="inline-flex items-center justify-end gap-1">
                          {formatPrice(s.live_buy_price ?? s.buy_price)}
                          {s.live_buy_price != null && (
                            <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse shrink-0" title="Live price" />
                          )}
                        </span>
                      </td>

                      {/* Spread % */}
                      <td className="px-2 py-2.5 text-right">
                        <span className="inline-flex items-center justify-end gap-1">
                          <SpreadValue spread={s.live_spread ?? s.spread_percent} level={s.level} />
                          {s.live_spread != null && (
                            <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse shrink-0" title="Live spread" />
                          )}
                        </span>
                      </td>

                      {/* Profit */}
                      <td className="px-2 py-2.5 text-right font-num text-emerald-400 font-semibold">
                        {formatProfit(s.profit)}
                      </td>

                      {/* Volume */}
                      <td className="px-2 py-2.5 text-right font-num text-slate-400 hidden xl:table-cell">
                        {formatVolume(s.volume)}
                      </td>

                      {/* Level */}
                      <td className="px-2 py-2.5">
                        <LevelBadge level={s.level} />
                      </td>

                      {/* Age — live price age when available, else signal age */}
                      <td className="px-2 py-2.5 text-right hidden md:table-cell">
                        {(() => {
                          const liveTs = s.live_buy_age != null && s.live_sell_age != null
                            ? Math.min(s.live_buy_age, s.live_sell_age)
                            : s.live_buy_age ?? s.live_sell_age ?? null
                          const ts = liveTs ?? s.analyzed_at
                          const ms = ts ? Date.now() - ts : Infinity
                          return (
                            <span
                              title={liveTs != null ? 'Age of live price data' : 'Time since spread last triggered'}
                              className={`text-xs font-num ${ ms < 30_000 ? 'text-emerald-400' : ms < 10 * 60_000 ? 'text-slate-400' : ms < 60 * 60_000 ? 'text-amber-400' : 'text-red-400' }`}
                            >
                              {formatAge(ts)}
                            </span>
                          )
                        })()}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>

          {/* ── Footer ── */}
          <div className="flex items-center justify-between px-4 py-2 border-t border-[#1e2d45] bg-[#0d1321] text-xs text-slate-500 flex-shrink-0">
            <span className="font-num">
              {sortedRows.length} signal{sortedRows.length !== 1 ? 's' : ''}
              {filteredRows.length !== rows.length && ` (filtered from ${rows.length})`}
            </span>
            <span className="font-num">
              Auto-refresh every {process.env.NEXT_PUBLIC_STREAM_INTERVAL || '2'}s
            </span>
          </div>
        </div>

        {/* ── Prices panel ── */}
        {selected && (
          <div className="w-[420px] flex-shrink-0 border-l border-[#1e2d45] bg-[#0a0e17] overflow-hidden flex flex-col">
            <PricesPanel key={selected.key} row={selected} onClose={() => setSelected(null)} />
          </div>
        )}
      </div>
    </div>
  )
}

// ── Sub-components ────────────────────────────────────────────────────────────

function Th({ children, className = '' }: { children?: React.ReactNode; className?: string }) {
  return (
    <th
      className={`text-left px-2 py-2.5 text-xs font-semibold text-slate-500 uppercase tracking-wider
        whitespace-nowrap select-none ${className}`}
    >
      {children}
    </th>
  )
}

function SortTh({
  label,
  sortKey,
  current,
  dir,
  onSort,
  className = '',
}: {
  label: string
  sortKey: SortKey
  current: SortKey
  dir: SortDir
  onSort: (k: SortKey) => void
  className?: string
}) {
  const active = current === sortKey
  return (
    <th
      onClick={() => onSort(sortKey)}
      className={`text-right px-2 py-2.5 text-xs font-semibold uppercase tracking-wider
        whitespace-nowrap select-none cursor-pointer
        ${active ? 'text-sky-400' : 'text-slate-500 hover:text-slate-300'} ${className}`}
    >
      <span className="inline-flex items-center gap-0.5 justify-end">
        {label}
        {active ? (
          dir === 'desc' ? <ChevronDown size={12} /> : <ChevronUp size={12} />
        ) : (
          <ChevronUp size={12} className="opacity-20" />
        )}
      </span>
    </th>
  )
}

function SpreadValue({ spread, level }: { spread: number; level: string }) {
  const isVeryHigh = level === 'VERY_HIGH'
  const isHigh = level === 'High' || level === 'HIGH'
  return (
    <span
      className={`font-num font-bold text-sm
        ${isVeryHigh ? 'text-red-400 glow-red' : ''}
        ${isHigh ? 'text-amber-400 glow-amber' : ''}
        ${!isVeryHigh && !isHigh ? 'text-sky-400' : ''}
      `}
    >
      {formatSpread(spread)}
    </span>
  )
}

function StatPill({ label, value, total }: { label: string; value: string | number; total?: number }) {
  return (
    <div className="flex items-center gap-1.5 text-xs">
      <span className="text-slate-500">{label}</span>
      <span className="font-num font-semibold text-white">
        {value}
        {total !== undefined && total !== value && (
          <span className="text-slate-500 font-normal">/{total}</span>
        )}
      </span>
    </div>
  )
}
