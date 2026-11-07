export interface ChainInfo {
  chain_name: string
  contract_address: string
  withdraw_enabled: boolean
  deposit_enabled: boolean
  withdrawal_fee: number
  withdrawal_fee_in_usdt: number
  cex_withdrawal_fee: number
  cex_deposit_fee: number
  total_tax_fee_in_token: number
  token_source_name: string
  deposit_enabled_changed_at?: string | null
  withdraw_enabled_changed_at?: string | null
}

export interface TickerInfo {
  orderbook: {
    bids: [number, number][]
    asks: [number, number][]
  }
  contract_size: number
  source_name: string
  base_asset: string
  quote_asset: string
  symbol: string
  updated_at: string | null
  get_at: number | null
  chains: ChainInfo[]
}

export interface TickersData {
  spot: Record<string, TickerInfo>
  futures: Record<string, TickerInfo>
  dex: Record<string, TickerInfo>
}

export type SpreadLevel = 'Low' | 'High' | 'VERY_HIGH' | string

export interface SpreadSignal {
  global_token_id: string
  symbol: string
  buy_from: string
  sell_to: string
  buy_price: number
  sell_price: number
  spread_percent: number
  profit: number
  profit_before_fee: number
  applied_fee: number
  volume: number
  level: SpreadLevel
  analyzed_at: number
  tickers_data: TickersData
  chains: ChainInfo[]
  sources: Record<string, string>
  symbols: Record<string, string>
  // Live price fields (injected from price streams, optional)
  live_buy_price?: number
  live_sell_price?: number
  live_spread?: number
  live_buy_age?: number
  live_sell_age?: number
}

export interface SpreadRow {
  key: string
  signal: SpreadSignal
}

export interface TokenPrice {
  exchange: string
  exchangeKey: string
  type: 'spot' | 'futures' | 'dex'
  bestBid: number | null
  bestAsk: number | null
  spread: number | null
  symbol: string
  get_at: number | null
  chains: ChainInfo[]
}
