import Redis from 'ioredis'
import { Unpackr } from 'msgpackr'
import type { SpreadRow } from './types'
import type { TokenPriceRow } from './prices'

declare global {
  // eslint-disable-next-line no-var
  var _redis: Redis | undefined
  // eslint-disable-next-line no-var
  var _redisBuf: Redis | undefined
}

function createRedis(): Redis {
  const client = new Redis({
    host: process.env.REDIS_HOST || 'redis',
    port: parseInt(process.env.REDIS_PORT || '6379'),
    maxRetriesPerRequest: 3,
    connectTimeout: 5000,
    lazyConnect: false,
  })
  client.on('error', (err) => console.error('[Redis] error:', err.message))
  return client
}

function createRedisBuf(): Redis {
  // @ts-ignore – returnBuffers is valid at constructor level
  const client = new Redis({
    host: process.env.REDIS_HOST || 'redis',
    port: parseInt(process.env.REDIS_PORT || '6379'),
    maxRetriesPerRequest: 3,
    connectTimeout: 5000,
    lazyConnect: false,
    returnBuffers: true,
  })
  client.on('error', (err) => console.error('[RedisBuf] error:', err.message))
  return client
}

export function getRedis(): Redis {
  if (!global._redis) global._redis = createRedis()
  return global._redis
}

function getRedisBuf(): Redis {
  if (!global._redisBuf) global._redisBuf = createRedisBuf()
  return global._redisBuf
}

// ── Spreads ────────────────────────────────────────────────────────────────────

export async function getAllSpreads(): Promise<SpreadRow[]> {
  const client = getRedis()
  const keys = await client.keys('arbitrage:*')
  if (keys.length === 0) return []
  const values = await client.mget(...keys)
  const result: SpreadRow[] = []
  for (let i = 0; i < keys.length; i++) {
    const raw = values[i]
    if (!raw) continue
    try {
      const signal = JSON.parse(raw)
      if (!signal.buy_from || !signal.sell_to || !signal.symbol) continue
      result.push({ key: keys[i], signal })
    } catch { /* skip */ }
  }

  // Enrich with live prices from price streams
  try {
    const snapshot = await getStreamPriceSnapshot()
    const byToken = new Map<string, Record<string, PriceEntry>>()
    for (const row of snapshot) byToken.set(row.global_token_id, row.exchanges)

    for (const row of result) {
      const s = row.signal
      const exchanges = byToken.get(s.global_token_id)
      if (!exchanges) continue
      const buyEx  = exchanges[s.buy_from]
      const sellEx = exchanges[s.sell_to]
      if (buyEx) {
        s.live_buy_price = buyEx.ask ?? buyEx.bid ?? undefined
        s.live_buy_age   = buyEx.get_at ?? undefined
      }
      if (sellEx) {
        s.live_sell_price = sellEx.bid ?? sellEx.ask ?? undefined
        s.live_sell_age   = sellEx.get_at ?? undefined
      }
      if (s.live_buy_price && s.live_sell_price && s.live_buy_price > 0) {
        s.live_spread = ((s.live_sell_price - s.live_buy_price) / s.live_buy_price) * 100
      }
    }
  } catch (e) {
    console.error('[getAllSpreads] live price enrichment failed:', e)
  }

  return result
}

// ── Source map (global_source_id → source_name, market_type) ──────────────────

let _sourceMap: Map<string, { sourceName: string; marketType: string }> | null = null
let _sourceMapTs = 0
const SOURCE_MAP_TTL = 5 * 60 * 1000

async function getSourceMap() {
  const now = Date.now()
  if (_sourceMap && now - _sourceMapTs < SOURCE_MAP_TTL) return _sourceMap

  const client = getRedis()
  const map = new Map<string, { sourceName: string; marketType: string }>()

  // FIX-008: Read from both available_tokens (ephemeral) and source_registry (persistent).
  // available_tokens entries are removed by price collector after pickup, so most tokens
  // would be invisible. source_registry is append-only and always has the full picture.
  const [availableKeys, registryKeys] = await Promise.all([
    client.keys('available_tokens:*'),
    client.keys('source_registry:*'),
  ])

  const parseKey = (key: string, pattern: 'available' | 'registry') => {
    const parts = key.split(':')
    if (pattern === 'available') {
      // available_tokens:{sourceName}:{sourceId}:{MARKET_TYPE}
      return { sourceName: parts[1]?.toLowerCase(), marketType: parts[3]?.toLowerCase() }
    } else {
      // source_registry:{sourceName}:{MARKET_TYPE}
      return { sourceName: parts[1]?.toLowerCase(), marketType: parts[2]?.toLowerCase() }
    }
  }

  const processKeys = async (keys: string[], pattern: 'available' | 'registry') => {
    await Promise.all(keys.map(async (key) => {
      const { sourceName, marketType } = parseKey(key, pattern)
      if (!sourceName || !marketType) return
      const members = await client.smembers(key)
      for (const raw of members) {
        try {
          const obj = JSON.parse(raw)
          if (obj.global_source_id) map.set(obj.global_source_id, { sourceName, marketType })
        } catch { /* skip */ }
      }
    }))
  }

  await Promise.all([
    processKeys(availableKeys, 'available'),
    processKeys(registryKeys,  'registry'),
  ])

  _sourceMap = map
  _sourceMapTs = now
  console.log(`[SourceMap] built: ${map.size} entries from ${availableKeys.length} available + ${registryKeys.length} registry keys`)
  return map
}

// ── Price snapshot from streams (incremental) ─────────────────────────────────

interface PriceEntry { bid: number | null; ask: number | null; get_at: number | null; symbol: string }
interface TokenData  { symbol: string; exchanges: Record<string, PriceEntry> }

declare global {
  var _priceMap: Map<string, TokenData> | undefined
  var _priceLastIds: { spot: string; futures: string } | undefined
  var _dexLastIds: Record<string, string> | undefined
  var _priceMapReady: boolean | undefined
  var _priceMapInitializing: Promise<void> | undefined
  var _lastRegistrySeedTs: number | undefined
}

const SNAPSHOT_TTL    = 3000
const INIT_READ_COUNT = 200000  // read full stream on first load

const _unpackr = new Unpackr({ useRecords: false })

// DEX stream configuration (plain-string Redis streams)
const DEX_CONFIGS = [
  { streamKey: 'dex_prices:gmgn', sourceKey: 'dex_gmgn' },
]

function applyStreamEntries(
  entries: [Buffer, Buffer[]][],
  sourceMap: Map<string, { sourceName: string; marketType: string }>,
  map: Map<string, TokenData>,
  mode: 'init' | 'incremental' = 'incremental',
) {
  // init (XREVRANGE): entries are newest→oldest, keep FIRST write per (gtId, colKey)
  // incremental (XRANGE): entries are oldest→newest, ALWAYS overwrite (last = newest)
  const up = _unpackr
  for (const [, fields] of entries) {
    const raw: Buffer = Array.isArray(fields)
      ? (fields[1] as unknown as Buffer)
      : (fields as unknown as Buffer)
    let d: Record<string, unknown>
    try { d = up.unpack(raw) } catch { continue }

    const gtId = d.global_token_id as string
    const gsId = d.global_source_id as string
    if (!gtId || !gsId) continue

    const source = sourceMap.get(gsId)
    if (!source) continue

    const colKey    = `${source.marketType}_${source.sourceName}`
    const rawSymbol = (d.symbol as string) || ''
    const symbol    = rawSymbol.replace(/_USDT$|\/USDT$|_USDC$|\/USDC$/, '')
    const getAt     = d.get_at != null ? Number(d.get_at as bigint | number) : null
    const ob        = d.orderbook as { bids?: unknown[][]; asks?: unknown[][] } | undefined
    const bid       = ob?.bids?.length ? parseFloat(String(ob.bids[0][0])) : null
    const ask       = ob?.asks?.length ? parseFloat(String(ob.asks[0][0])) : null

    if (!map.has(gtId)) map.set(gtId, { symbol, exchanges: {} })
    const row = map.get(gtId)!
    // Prefer canonical symbol: Q_USDT → "Q" beats QUSDT → "QUSDT"
    if (rawSymbol.includes('_') && row.symbol !== symbol) row.symbol = symbol
    const existing = row.exchanges[colKey]

    if (mode === 'init') {
      // Keep first write (= newest entry from XREVRANGE)
      if (!existing) row.exchanges[colKey] = { bid, ask, get_at: getAt, symbol: rawSymbol }
    } else {
      // Always overwrite (= last entry from XRANGE is the newest)
      row.exchanges[colKey] = { bid, ask, get_at: getAt, symbol: rawSymbol }
    }
  }
}

// Apply entries from a plain-string DEX stream (e.g. dex_prices:gmgn)
function applyDexStreamEntries(
  entries: [string, string[]][],
  sourceKey: string,
  map: Map<string, TokenData>,
  mode: 'init' | 'incremental' = 'incremental',
) {
  for (const [, fields] of entries) {
    // fields are interleaved: [key, value, key, value, ...]
    const obj: Record<string, string> = {}
    for (let i = 0; i + 1 < fields.length; i += 2) {
      obj[fields[i]] = fields[i + 1]
    }

    const gtId = obj["global_token_id"] || obj["token_id"]
    if (!gtId) continue

    const price     = obj['price']     ? parseFloat(obj['price'])     : null
    const getAt     = obj['timestamp'] ? parseInt(obj['timestamp'])   : null
    const rawSymbol = obj['symbol'] || ''
    const symbol    = rawSymbol.replace(/_USDT$|\/USDT$|_USDC$|\/USDC$/, '')

    if (!map.has(gtId)) map.set(gtId, { symbol, exchanges: {} })
    const row     = map.get(gtId)!
    const existing = row.exchanges[sourceKey]

    if (mode === 'init') {
      if (!existing) row.exchanges[sourceKey] = { bid: price, ask: price, get_at: getAt, symbol: rawSymbol }
    } else {
      row.exchanges[sourceKey] = { bid: price, ask: price, get_at: getAt, symbol: rawSymbol }
    }
  }
}

// ── Registry seeding: populate map with known exchange tokens (bid=null) ─────────

const REGISTRY_SEED_INTERVAL = 5 * 60 * 1000  // re-seed every 5 min

async function seedMapFromRegistry(map: Map<string, TokenData>): Promise<number> {
  const client = getRedis()
  const registryKeys = await client.keys('source_registry:*')
  let seeded = 0

  for (const key of registryKeys) {
    const parts = key.split(':')
    const sourceName = parts[1]?.toLowerCase()
    const marketType = parts[2]?.toLowerCase()
    if (!sourceName || !marketType) continue

    const colKey = `${marketType}_${sourceName}`
    const members = await client.smembers(key)

    for (const raw of members) {
      try {
        const obj = JSON.parse(raw)
        const gtId: string = obj.global_token_id
        const rawSymbol: string = obj.symbol || ''
        if (!gtId) continue

        const symbol = rawSymbol.replace(/_USDT$|\/USDT$|_USDC$|\/USDC$/, '')

        if (!map.has(gtId)) {
          map.set(gtId, { symbol, exchanges: {} })
        }
        const row = map.get(gtId)!
        // Prefer canonical symbol: Q_USDT → "Q" beats QUSDT → "QUSDT"
        if (rawSymbol.includes('_') && row.symbol !== symbol) row.symbol = symbol
        if (!row.exchanges[colKey]) {
          row.exchanges[colKey] = { bid: null, ask: null, get_at: null, symbol: rawSymbol }
          seeded++
        }
      } catch { /* skip */ }
    }
  }

  global._lastRegistrySeedTs = Date.now()
  return seeded
}

async function initPriceMap(): Promise<void> {
  const buf       = getRedisBuf()
  const client    = getRedis()
  const sourceMap = await getSourceMap()
  const map       = new Map<string, TokenData>()
  const ids       = { spot: '0', futures: '0' }

  // XREVRANGE gives newest-first; process as-is (get_at check keeps latest value)
  const [spotEntries, futEntries] = await Promise.all([
    buf.xrevrangeBuffer('prices:spot',    '+', '-', 'COUNT', String(INIT_READ_COUNT)) as Promise<[Buffer, Buffer[]][]>,
    buf.xrevrangeBuffer('prices:futures', '+', '-', 'COUNT', String(INIT_READ_COUNT)) as Promise<[Buffer, Buffer[]][]>,
  ])

  applyStreamEntries(spotEntries, sourceMap, map, 'init')
  applyStreamEntries(futEntries,  sourceMap, map, 'init')

  // Track newest IDs (first entry in xrevrange = newest)
  if (spotEntries.length > 0) ids.spot    = (spotEntries[0][0] as Buffer).toString()
  if (futEntries.length  > 0) ids.futures = (futEntries[0][0]  as Buffer).toString()

  // Initialize DEX streams (plain string format)
  const dexIds: Record<string, string> = {}
  for (const { streamKey, sourceKey } of DEX_CONFIGS) {
    try {
      const dexEntries = await client.xrevrange(streamKey, '+', '-', 'COUNT', String(INIT_READ_COUNT)) as unknown as [string, string[]][]
      applyDexStreamEntries(dexEntries, sourceKey, map, 'init')
      dexIds[streamKey] = dexEntries.length > 0 ? dexEntries[0][0] : '0'
      console.log(`[PriceMap] dex ${streamKey}: ${dexEntries.length} entries`)
    } catch (e) {
      console.error(`[PriceMap] dex ${streamKey} error:`, e)
      dexIds[streamKey] = '0'
    }
  }

  // Seed all known registry tokens with null prices (show in table even without live prices)
  const seededCount = await seedMapFromRegistry(map)
  console.log(`[PriceMap] registry seed: ${seededCount} new entries`)

  global._priceMap     = map
  global._priceLastIds = ids
  global._dexLastIds   = dexIds
  global._priceMapReady = true
  console.log(`[PriceMap] init: ${map.size} tokens | spot=${spotEntries.length} futures=${futEntries.length}`)
}

let _snapshotCache: TokenPriceRow[] | null = null
let _snapshotCacheTs = 0

export async function getStreamPriceSnapshot(): Promise<TokenPriceRow[]> {
  const now = Date.now()
  if (_snapshotCache && now - _snapshotCacheTs < SNAPSHOT_TTL) return _snapshotCache

  // Ensure map is initialized (shared promise — only one init in flight)
  if (!global._priceMapReady) {
    if (!global._priceMapInitializing) global._priceMapInitializing = initPriceMap()
    await global._priceMapInitializing
  }

  // Incremental update: only new entries since last seen IDs
  const buf       = getRedisBuf()
  const client    = getRedis()
  const sourceMap = await getSourceMap()
  const ids       = global._priceLastIds!
  const map       = global._priceMap!

  const startSpot = ids.spot    !== '0' ? `(${ids.spot}`    : '0'
  const startFut  = ids.futures !== '0' ? `(${ids.futures}` : '0'

  const [spotNew, futNew] = await Promise.all([
    buf.xrangeBuffer('prices:spot',    startSpot, '+') as Promise<[Buffer, Buffer[]][]>,
    buf.xrangeBuffer('prices:futures', startFut,  '+') as Promise<[Buffer, Buffer[]][]>,
  ])

  if (spotNew.length > 0) {
    applyStreamEntries(spotNew, sourceMap, map)
    ids.spot = (spotNew[spotNew.length - 1][0] as Buffer).toString()
  }
  if (futNew.length > 0) {
    applyStreamEntries(futNew, sourceMap, map)
    ids.futures = (futNew[futNew.length - 1][0] as Buffer).toString()
  }

  // Incremental update: DEX streams
  const dexIds = global._dexLastIds!
  for (const { streamKey, sourceKey } of DEX_CONFIGS) {
    try {
      const lastId  = dexIds[streamKey] || '0'
      const startId = lastId !== '0' ? `(${lastId}` : '0'
      const dexNew  = await client.xrange(streamKey, startId, '+') as unknown as [string, string[]][]
      if (dexNew.length > 0) {
        applyDexStreamEntries(dexNew, sourceKey, map)
        dexIds[streamKey] = dexNew[dexNew.length - 1][0]
      }
    } catch (e) {
      console.error(`[PriceMap] dex incremental ${streamKey} error:`, e)
    }
  }

  // Periodically re-seed registry tokens to pick up newly listed tokens
  const lastSeed = global._lastRegistrySeedTs || 0
  if (now - lastSeed > REGISTRY_SEED_INTERVAL) {
    seedMapFromRegistry(map).then((n) => {
      if (n > 0) console.log(`[PriceMap] periodic re-seed: ${n} new entries`)
    }).catch((e) => console.error('[PriceMap] registry re-seed error:', e))
  }

  const rows: TokenPriceRow[] = Array.from(map.entries())
    .map(([global_token_id, data]) => ({ global_token_id, ...data }))
    .sort((a, b) => {
      // Sort by number of exchanges with actual prices (desc), then alphabetically
      const aC = Object.values(a.exchanges).filter(e => e.bid !== null).length
      const bC = Object.values(b.exchanges).filter(e => e.bid !== null).length
      return bC !== aC ? bC - aC : a.symbol.localeCompare(b.symbol)
    })

  _snapshotCache   = rows
  _snapshotCacheTs = now
  return rows
}
