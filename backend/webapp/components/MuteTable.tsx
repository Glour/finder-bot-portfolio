'use client'

import { useEffect, useState } from 'react'

interface MuteRow {
  id: string
  symbol: string
  mute_type: 'PERMANENT' | 'TEMPORARY'
  expires_at: string | null
  chain_name: string | null
  contract_address: string | null
  created_at: string
  global_token_id: string | null
}

function fmtDate(iso: string | null): string {
  if (!iso) return '—'
  const d = new Date(iso)
  return d.toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit', hour: '2-digit', minute: '2-digit', timeZone: 'UTC' }) + ' UTC'
}

function isExpired(expires_at: string | null): boolean {
  if (!expires_at) return false
  return new Date(expires_at) < new Date()
}

export default function MuteTable() {
  const [rows, setRows] = useState<MuteRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [hideExpired, setHideExpired] = useState(false)

  useEffect(() => {
    fetch('/api/mutes')
      .then((r) => r.json())
      .then((d) => {
        setRows(d.data || [])
        setLoading(false)
      })
      .catch(() => {
        setError('Failed to load mute list')
        setLoading(false)
      })
  }, [])

  const filtered = rows.filter((r) => {
    if (hideExpired && isExpired(r.expires_at)) return false
    if (search) {
      const q = search.toLowerCase()
      if (
        !r.symbol?.toLowerCase().includes(q) &&
        !r.contract_address?.toLowerCase().includes(q) &&
        !r.chain_name?.toLowerCase().includes(q)
      ) return false
    }
    return true
  })

  const permanent = filtered.filter((r) => r.mute_type === 'PERMANENT').length
  const temporary = filtered.filter((r) => r.mute_type === 'TEMPORARY').length
  const expired   = filtered.filter((r) => isExpired(r.expires_at)).length

  if (loading) return (
    <div className="flex items-center justify-center h-40 text-slate-500 text-sm">Loading mute list…</div>
  )
  if (error) return (
    <div className="flex items-center justify-center h-40 text-red-400 text-sm">{error}</div>
  )

  return (
    <div className="flex flex-col h-full overflow-hidden">
      {/* Toolbar */}
      <div className="flex items-center gap-3 px-4 py-2 border-b border-slate-800 shrink-0 flex-wrap">
        <input
          type="text"
          placeholder="Search symbol / contract / chain…"
          className="bg-slate-800 border border-slate-700 rounded px-3 py-1 text-sm text-slate-200 placeholder-slate-500 w-60 focus:outline-none focus:border-slate-500"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <label className="flex items-center gap-1.5 text-xs text-slate-400 cursor-pointer select-none">
          <input
            type="checkbox"
            checked={hideExpired}
            onChange={(e) => setHideExpired(e.target.checked)}
            className="accent-blue-500"
          />
          Hide expired
        </label>
        <span className="text-xs text-slate-500 ml-auto">
          {filtered.length} entries · {permanent} permanent · {temporary} temporary
          {expired > 0 && <span className="text-slate-600"> · {expired} expired</span>}
        </span>
      </div>

      {/* Table */}
      <div className="overflow-auto flex-1">
        <table className="w-full text-sm border-collapse">
          <thead className="sticky top-0 z-10 bg-slate-900">
            <tr className="text-left text-xs text-slate-500 border-b border-slate-800">
              <th className="px-3 py-2 font-medium">Symbol</th>
              <th className="px-3 py-2 font-medium">Type</th>
              <th className="px-3 py-2 font-medium">Chain</th>
              <th className="px-3 py-2 font-medium">Contract</th>
              <th className="px-3 py-2 font-medium">Expires</th>
              <th className="px-3 py-2 font-medium">Added</th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 && (
              <tr>
                <td colSpan={6} className="px-3 py-8 text-center text-slate-600 text-sm">No entries</td>
              </tr>
            )}
            {filtered.map((row) => {
              const expired = isExpired(row.expires_at)
              return (
                <tr
                  key={row.id}
                  className={`border-b border-slate-800/60 hover:bg-slate-800/40 transition-colors ${expired ? 'opacity-40' : ''}`}
                >
                  <td className="px-3 py-2 font-mono font-semibold text-slate-200">
                    {row.symbol}
                    {expired && <span className="ml-1.5 text-xs text-slate-500 font-normal">(expired)</span>}
                  </td>
                  <td className="px-3 py-2">
                    <span className={`px-1.5 py-0.5 rounded text-xs font-medium ${
                      row.mute_type === 'PERMANENT'
                        ? 'bg-red-900/60 text-red-300'
                        : 'bg-yellow-900/60 text-yellow-300'
                    }`}>
                      {row.mute_type === 'PERMANENT' ? 'Permanent' : 'Temporary'}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-slate-400 text-xs">{row.chain_name || '—'}</td>
                  <td className="px-3 py-2 font-mono text-xs text-slate-500 max-w-[200px] truncate" title={row.contract_address || ''}>
                    {row.contract_address
                      ? `${row.contract_address.slice(0, 8)}…${row.contract_address.slice(-6)}`
                      : '—'}
                  </td>
                  <td className="px-3 py-2 text-xs text-slate-400">{fmtDate(row.expires_at)}</td>
                  <td className="px-3 py-2 text-xs text-slate-500">{fmtDate(row.created_at)}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
