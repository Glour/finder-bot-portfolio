'use client'

import { useState, useCallback, useEffect } from 'react'
import { X, Clock, AlertTriangle, CheckCircle2, ArrowRight, ExternalLink, Copy, Check } from 'lucide-react'
import type { SpreadRow } from '@/lib/types'
import type { ExchangePrice } from '@/lib/prices'
import {
  extractTokenPrices,
  formatPrice,
  formatSpread,
  formatAge,
  isStale,
  getExchangeColors,
  getExchangeName,
  getMarketType,
  getExchangeLabel,
  formatProfit,
  formatVolume,
  getExchangeUrl,
  getGmgnUrl,
} from '@/lib/utils'
import ExchangeBadge from './ExchangeBadge'
import LevelBadge from './LevelBadge'

interface Props {
  row: SpreadRow
  onClose: () => void
}

// Format ISO timestamp as 'Nd / Nh / Nm / Ns ago'
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
export default function PricesPanel({ row, onClose }: Props) {
  const { signal } = row
  const [copiedAddr, setCopiedAddr] = useState<string | null>(null)
  const [liveExchanges, setLiveExchanges] = useState<Record<string, ExchangePrice> | null>(null)

  const signalPrices = extractTokenPrices(signal)

  // Build prices list: prefer liveExchanges (fresh from stream), fallback to signal.tickers_data
  const prices = liveExchanges
    ? Object.entries(liveExchanges)
        .map(([exchangeKey, ep]) => ({
          exchange: getExchangeLabel(exchangeKey),
          exchangeKey,
          type: getMarketType(exchangeKey).toLowerCase() as 'spot' | 'futures' | 'dex',
          bestBid: ep.bid,
          bestAsk: ep.ask,
          spread: null,
          symbol: ep.symbol || signalPrices.find((p) => p.exchangeKey === exchangeKey)?.symbol || '',
          get_at: ep.get_at,
          chains: [],
        }))
        .sort((a, b) => {
          const order = ['spot', 'futures', 'dex']
          const diff = order.indexOf(a.type) - order.indexOf(b.type)
          return diff !== 0 ? diff : a.exchange.localeCompare(b.exchange)
        })
    : signalPrices

  // Poll fresh prices from stream every 3s (avoids stale signal.tickers_data)
  useEffect(() => {
    let cancelled = false
    const fetchPrices = async () => {
      try {
        const res = await fetch(`/api/prices/token?id=${encodeURIComponent(signal.global_token_id)}`)
        if (!res.ok || cancelled) return
        const json = await res.json()
        if (json.data?.exchanges && !cancelled) setLiveExchanges(json.data.exchanges)
      } catch {}
    }
    fetchPrices()
    const t = setInterval(fetchPrices, 3000)
    return () => { cancelled = true; clearInterval(t) }
  }, [signal.global_token_id])

  // Live buy/sell prices from stream (ask for buy, bid for sell; fallback to signal snapshot)
  const liveBuyPrice  = liveExchanges?.[signal.buy_from]?.ask  ?? liveExchanges?.[signal.buy_from]?.bid  ?? signal.buy_price
  const liveSellPrice = liveExchanges?.[signal.sell_to]?.bid   ?? liveExchanges?.[signal.sell_to]?.ask   ?? signal.sell_price
  const pricesAreLive = liveExchanges != null

  // Recalculate spread/profit from live prices when available
  const liveSpread = (pricesAreLive && liveBuyPrice && liveSellPrice && liveBuyPrice > 0)
    ? ((liveSellPrice - liveBuyPrice) / liveBuyPrice) * 100
    : null

  const buyColors = getExchangeColors(signal.buy_from)
  const sellColors = getExchangeColors(signal.sell_to)

  // Build GMGN URL from chains data for a given sourceKey
  const getHrefForSource = (srcKey: string): string | undefined => {
    const { exchange } = srcKey.includes('_') ? { exchange: srcKey.split('_').slice(1).join('_') } : { exchange: srcKey }
    if (exchange !== 'gmgn') return undefined
    const chain = (signal.chains || []).find(
      (c) => (c.token_source_name || '').toLowerCase() === 'gmgn' && c.contract_address
    )
    if (!chain) return undefined
    return getGmgnUrl(chain.chain_name, chain.contract_address) ?? undefined
  }

  // Unique contract addresses from chains
  const contractAddrs = Array.from(
    new Map(
      (signal.chains || [])
        .filter((c) => c.contract_address)
        .map((c) => [c.contract_address.toLowerCase(), c.contract_address])
    ).values()
  )

  const handleCopyAddr = useCallback((addr: string) => {
    navigator.clipboard.writeText(addr).then(() => {
      setCopiedAddr(addr)
      setTimeout(() => setCopiedAddr(null), 2000)
    })
  }, [])

  return (
    <div className="flex flex-col h-full animate-[slideIn_0.2s_ease-out]">
      {/* Panel header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-[#1e2d45] bg-[#0d1321]">
        <div className="flex items-center gap-3">
          <div>
            <div className="flex items-center gap-2">
              <span className="text-lg font-bold text-white font-num">{signal.symbol}</span>
              <span className="text-slate-500 text-sm">/USDT</span>
              <LevelBadge level={signal.level} />
            </div>
            {/* Contract addresses — copyable on click */}
            {contractAddrs.length > 0 ? (
              <div className="flex flex-col gap-0.5 mt-0.5">
                {contractAddrs.map((addr) => {
                  const copied = copiedAddr === addr
                  return (
                    <button
                      key={addr}
                      onClick={() => handleCopyAddr(addr)}
                      title={addr}
                      className="flex items-center gap-1.5 text-xs font-num text-slate-400 hover:text-white transition-colors group text-left"
                    >
                      <span>{addr.slice(0, 8)}…{addr.slice(-6)}</span>
                      {copied
                        ? <Check size={11} className="text-emerald-400 shrink-0" />
                        : <Copy size={11} className="opacity-0 group-hover:opacity-50 shrink-0 transition-opacity" />
                      }
                      {copied && (
                        <span className="text-emerald-400 text-[10px]">copied!</span>
                      )}
                    </button>
                  )
                })}
              </div>
            ) : (
              <div className="text-xs text-slate-600 mt-0.5 font-num">
                {signal.global_token_id}
              </div>
            )}
          </div>
        </div>
        <button
          onClick={onClose}
          className="p-1.5 rounded hover:bg-white/10 text-slate-400 hover:text-white transition-colors"
        >
          <X size={16} />
        </button>
      </div>

      {/* Signal summary */}
      <div className="px-4 py-3 border-b border-[#1e2d45] bg-[#0a0f1c]">
        {/* Route */}
        <div className="flex items-center gap-2 mb-3">
          <ExchangeBadge sourceKey={signal.buy_from} size="md" symbol={signal.symbol} href={getHrefForSource(signal.buy_from)} />
          <ArrowRight size={14} className="text-slate-500" />
          <ExchangeBadge sourceKey={signal.sell_to} size="md" symbol={signal.symbol} href={getHrefForSource(signal.sell_to)} />
        </div>

        {/* Key metrics */}
        <div className="grid grid-cols-2 gap-2">
          <MetricCard label="Spread" value={liveSpread != null ? formatSpread(liveSpread) : formatSpread(signal.spread_percent)} highlight live={liveSpread != null} />
          <MetricCard label="Profit" value={formatProfit(signal.profit)} positive />
          <MetricCard
            label="Buy"
            value={formatPrice(liveBuyPrice)}
            live={pricesAreLive}
            sub={<ExchangeBadge sourceKey={signal.buy_from} showMarket={false} symbol={signal.symbol} href={getHrefForSource(signal.buy_from)} />}
          />
          <MetricCard
            label="Sell"
            value={formatPrice(liveSellPrice)}
            live={pricesAreLive}
            sub={<ExchangeBadge sourceKey={signal.sell_to} showMarket={false} symbol={signal.symbol} href={getHrefForSource(signal.sell_to)} />}
          />
          <MetricCard label="Volume" value={formatVolume(signal.volume)} />
          <MetricCard
            label="Signal age"
            value={`${formatAge(signal.analyzed_at)} ago`}
            stale={isStale(signal.analyzed_at, 120_000)}
            hint="Time since spread last passed trigger filter. If > 10 min — price data is stale."
          />
        </div>
      </div>

      {/* Prices table */}
      <div className="flex-1 overflow-y-auto">
        <div className="px-4 pt-3 pb-1">
          <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">
            Prices by Exchange
          </h3>
        </div>

        <table className="w-full text-sm">
          <thead>
            <tr className="text-xs text-slate-500 uppercase tracking-wider">
              <th className="text-left pl-4 pr-2 py-2 font-medium">Exchange</th>
              <th className="text-right px-2 py-2 font-medium">Best Bid</th>
              <th className="text-right px-2 py-2 font-medium">Best Ask</th>
              <th className="text-right pl-2 pr-4 py-2 font-medium">Age</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[#1e2d45]/50">
            {prices.map((p) => {
              const stale = isStale(p.get_at, 60_000)
              const colors = getExchangeColors(p.exchangeKey)
              const isActiveBuy = p.exchangeKey === signal.buy_from
              const isActiveSell = p.exchangeKey === signal.sell_to

              return (
                <tr
                  key={p.exchangeKey}
                  className={`
                    ${isActiveBuy || isActiveSell ? 'bg-white/[0.03]' : ''}
                  `}
                >
                  <td className="pl-4 pr-2 py-2.5">
                    <div className="flex items-center gap-2">
                      {(isActiveBuy || isActiveSell) && (
                        <span
                          className={`w-1 h-4 rounded-full ${
                            isActiveBuy ? buyColors.bg.replace('/15', '') : sellColors.bg.replace('/15', '')
                          }`}
                        />
                      )}
                      <div>
                        {(() => {
                          const url = getHrefForSource(p.exchangeKey) ?? getExchangeUrl(p.exchangeKey, signal.symbol)
                          return url ? (
                            <a
                              href={url}
                              target="_blank"
                              rel="noopener noreferrer"
                              onClick={(e) => e.stopPropagation()}
                              className={`inline-flex items-center gap-1 font-medium ${colors.text} text-xs hover:opacity-70 transition-opacity`}
                            >
                              {p.exchange}
                              <ExternalLink size={9} className="opacity-50 shrink-0" />
                            </a>
                          ) : (
                            <div className={`font-medium ${colors.text} text-xs`}>{p.exchange}</div>
                          )
                        })()}
                        {p.symbol && (
                          <div className="text-xs text-slate-600 font-num">{p.symbol}</div>
                        )}
                      </div>
                      {(isActiveBuy || isActiveSell) && (
                        <span className={`text-xs px-1 py-0.5 rounded font-num
                          ${isActiveBuy ? 'bg-emerald-500/15 text-emerald-400' : 'bg-sky-500/15 text-sky-400'}`}>
                          {isActiveBuy ? 'BUY' : 'SELL'}
                        </span>
                      )}
                    </div>
                  </td>
                  <td className="px-2 py-2.5 text-right font-num text-emerald-400">
                    {formatPrice(p.bestBid)}
                  </td>
                  <td className="px-2 py-2.5 text-right font-num text-red-400">
                    {formatPrice(p.bestAsk)}
                  </td>
                  <td className="pl-2 pr-4 py-2.5 text-right">
                    {p.get_at ? (
                      <span
                        className={`flex items-center justify-end gap-1 text-xs font-num
                          ${stale ? 'text-red-400' : 'text-slate-400'}`}
                      >
                        {stale ? (
                          <AlertTriangle size={11} />
                        ) : (
                          <CheckCircle2 size={11} className="text-emerald-500" />
                        )}
                        {formatAge(p.get_at)}
                      </span>
                    ) : (
                      <span className="text-xs text-slate-600">—</span>
                    )}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>

        {prices.length === 0 && (
          <div className="flex flex-col items-center justify-center py-12 text-slate-500">
            <AlertTriangle size={24} className="mb-2" />
            <p className="text-sm">No price data available</p>
          </div>
        )}

        {/* Chains section */}
        {signal.chains && signal.chains.length > 0 && (
          <div className="px-4 py-3 mt-2 border-t border-[#1e2d45]">
            <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">
              Chains ({signal.chains.length})
            </h3>
            <div className="space-y-1.5">
              {signal.chains.map((chain, i) => {
                const srcKey = `spot_${(chain.token_source_name || '').toLowerCase()}`
                const exColors = getExchangeColors(srcKey)
                const exName = getExchangeName(srcKey)
                const feeUsdt = chain.withdrawal_fee_in_usdt ?? 0
                return (
                  <div
                    key={i}
                    className="bg-[#0d1321] rounded px-3 py-2 text-xs border border-[#1e2d45]"
                  >
                    {/* Exchange label */}
                    <div className="flex items-center justify-between mb-1.5">
                      <span className={`font-semibold text-[11px] ${exColors.text}`}>
                        {exName}
                      </span>
                      <div className="flex items-center gap-2 font-num">
                        <span className={chain.deposit_enabled ? 'text-emerald-400' : 'text-red-400'}>
                          DEP {chain.deposit_enabled ? '✓' : '✗'}
                          {fmtChangedAt(chain.deposit_enabled_changed_at) && (
                            <span className="text-slate-500 ml-0.5 text-[9px]">{fmtChangedAt(chain.deposit_enabled_changed_at)}</span>
                          )}
                        </span>
                        <span className={chain.withdraw_enabled ? 'text-emerald-400' : 'text-red-400'}>
                          WD {chain.withdraw_enabled ? '✓' : '✗'}
                          {fmtChangedAt(chain.withdraw_enabled_changed_at) && (
                            <span className="text-slate-500 ml-0.5 text-[9px]">{fmtChangedAt(chain.withdraw_enabled_changed_at)}</span>
                          )}
                        </span>
                      </div>
                    </div>
                    {/* Chain details */}
                    <div className="flex items-center justify-between text-slate-500">
                      <span className="capitalize">{chain.chain_name}</span>
                      <div className="flex items-center gap-2 font-num">
                        {feeUsdt > 0 && (
                          <span className="text-amber-400 text-[10px]">fee ${feeUsdt.toFixed(2)}</span>
                        )}
                        {chain.contract_address && (() => {
                          const addr = chain.contract_address
                          const copied = copiedAddr === addr
                          return (
                            <button
                              onClick={() => handleCopyAddr(addr)}
                              title={addr}
                              className="flex items-center gap-1 text-[10px] font-num text-slate-500 hover:text-slate-300 transition-colors group"
                            >
                              <span>{addr.slice(0, 6)}…{addr.slice(-4)}</span>
                              {copied
                                ? <Check size={9} className="text-emerald-400 shrink-0" />
                                : <Copy size={9} className="opacity-0 group-hover:opacity-60 shrink-0 transition-opacity" />
                              }
                            </button>
                          )
                        })()}
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        )}

        <div className="h-6" />
      </div>
    </div>
  )
}

// ── Small metric card ──────────────────────────────────────────────────────────

function MetricCard({
  label,
  value,
  sub,
  highlight,
  positive,
  stale,
  hint,
  live,
}: {
  label: string
  value: string
  sub?: React.ReactNode
  highlight?: boolean
  positive?: boolean
  stale?: boolean
  hint?: string
  live?: boolean
}) {
  return (
    <div className="bg-[#0d1321] rounded px-3 py-2 border border-[#1e2d45]" title={hint}>
      <div className="text-xs text-slate-500 mb-0.5 flex items-center gap-1">
        {label}
        {live && <span className="inline-block w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" title="Live price" />}
      </div>
      <div
        className={`font-num font-semibold text-sm
          ${highlight ? 'text-sky-400' : ''}
          ${positive ? 'text-emerald-400' : ''}
          ${stale ? 'text-amber-400' : ''}
          ${!highlight && !positive && !stale ? 'text-white' : ''}
        `}
      >
        {value}
      </div>
      {sub && <div className="mt-1">{sub}</div>}
    </div>
  )
}
