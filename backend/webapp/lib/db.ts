import { Pool } from 'pg'

let _pool: Pool | null = null

export function getPool(): Pool {
  if (!_pool) {
    _pool = new Pool({
      host:     process.env.DB_HOST     || 'database',
      port:     parseInt(process.env.DB_PORT || '5432'),
      user:     process.env.DB_USER     || 'finder',
      password: process.env.DB_PASSWORD || 'local-only',
      database: process.env.DB_NAME     || 'finder',
      max: 3,
      idleTimeoutMillis: 30000,
    })
  }
  return _pool
}

export interface TokenChainInfo {
  chain_name: string
  contract_address: string
  deposit_enabled: boolean | null
  withdraw_enabled: boolean | null
  deposit_enabled_changed_at: string | null
  withdraw_enabled_changed_at: string | null
}

export interface TokenTradingPair {
  source_name: string
  market_type: string
  symbol: string
}

export interface TokenDetail {
  global_token_id: string
  symbol: string
  chains: TokenChainInfo[]
  trading_pairs: TokenTradingPair[]
}

// Chain name normalization: DB name → display name
const CHAIN_ALIAS: Record<string, string> = {
  'ethereum':                'ETH',
  'binance-smart-chain':     'BSC',
  'binance smart chain':     'BSC',
  'bsc':                     'BSC',
  'arbitrum':                'ARB',
  'arbitrum one':            'ARB',
  'polygon':                 'POLYGON',
  'avalanche':               'AVAX',
  'avalanche c-chain':       'AVAX',
  'optimism':                'OP',
  'base':                    'BASE',
  'solana':                  'SOL',
  'ton':                     'TON',
  'sui':                     'SUI',
  'aptos':                   'APT',
  'tron':                    'TRX',
  'ordinals':                'BTC',
  'bitcoin':                 'BTC',
}

function normalizeChainName(name: string): string {
  return CHAIN_ALIAS[name.toLowerCase()] ?? name.toUpperCase()
}

export async function getTokenDetail(globalTokenId: string): Promise<TokenDetail | null> {
  const pool = getPool()

  // DISTINCT ON (chain + lowercase contract) to deduplicate across exchanges
  const { rows } = await pool.query<{
    symbol: string
    chain_name: string
    contract_address: string
    deposit_enabled: boolean | null
    withdraw_enabled: boolean | null
    deposit_enabled_changed_at: string | null
    withdraw_enabled_changed_at: string | null
  }>(
    `SELECT DISTINCT ON (c.name, lower(COALESCE(tc.contract_address, '')))
       gt.symbol,
       c.name       AS chain_name,
       LOWER(tc.contract_address) AS contract_address,
       tc.deposit_enabled,
       tc.withdraw_enabled,
       tc.deposit_enabled_changed_at,
       tc.withdraw_enabled_changed_at
     FROM global_tokens gt
     JOIN token_chains tc ON tc.global_token_id = gt.id
     JOIN chains c        ON c.id = tc.chain_id
     WHERE gt.id = $1
       AND gt.is_deleted = false
     ORDER BY c.name, lower(COALESCE(tc.contract_address, '')), tc.deposit_enabled DESC NULLS LAST`,
    [globalTokenId]
  )

  // Also fetch trading pairs (exchange + symbol) via token_prices which links tokens to global_tokens
  const { rows: pairRows } = await pool.query<{
    source_name: string
    market_type: string
    symbol: string
  }>(
    `SELECT DISTINCT s.name AS source_name, tp.market_type, t.symbol
     FROM token_prices tp
     JOIN tokens t ON t.id = tp.token_id
     JOIN sources s ON s.id = t.source_id
     WHERE tp.global_token_id = $1
       AND s.type = 'CEX'
     ORDER BY s.name, tp.market_type`,
    [globalTokenId]
  )

  const globalToken = (
    await pool.query<{ symbol: string }>(
      `SELECT symbol FROM global_tokens WHERE id = $1 AND is_deleted = false`,
      [globalTokenId]
    )
  ).rows[0]

  if (!globalToken) return null

  return {
    global_token_id: globalTokenId,
    symbol: globalToken.symbol,
    chains: rows.map((r) => ({
      chain_name: normalizeChainName(r.chain_name),
      contract_address: r.contract_address || '',
      deposit_enabled: r.deposit_enabled,
      withdraw_enabled: r.withdraw_enabled,
      deposit_enabled_changed_at: r.deposit_enabled_changed_at ? new Date(r.deposit_enabled_changed_at).toISOString() : null,
      withdraw_enabled_changed_at: r.withdraw_enabled_changed_at ? new Date(r.withdraw_enabled_changed_at).toISOString() : null,
    })),
    trading_pairs: pairRows.filter(p => p.symbol), // filter out empty symbols
  }
}
