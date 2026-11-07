'use client'

import { getExchangeColors, getExchangeName, getMarketType, getExchangeUrl } from '@/lib/utils'
import { ExternalLink } from 'lucide-react'

interface Props {
  sourceKey: string
  showMarket?: boolean
  size?: 'sm' | 'md'
  symbol?: string
  href?: string  // override computed URL (e.g. GMGN needs contract address)
}

export default function ExchangeBadge({ sourceKey, showMarket = true, size = 'sm', symbol, href }: Props) {
  const colors = getExchangeColors(sourceKey)
  const name = getExchangeName(sourceKey)
  const market = getMarketType(sourceKey)
  const url = href ?? (symbol ? getExchangeUrl(sourceKey, symbol) : null)

  const sizeClass = size === 'sm' ? 'text-xs px-1.5 py-0.5' : 'text-sm px-2 py-1'

  const inner = (
    <>
      {name}
      {showMarket && (
        <span className="opacity-60 font-normal">
          {market}
        </span>
      )}
      {url && (
        <ExternalLink size={10} className="opacity-50 shrink-0" />
      )}
    </>
  )

  const baseClass = `inline-flex items-center gap-1 rounded border font-medium font-num
    ${colors.bg} ${colors.text} ${colors.border} ${sizeClass}`

  if (url) {
    return (
      <a
        href={url}
        target="_blank"
        rel="noopener noreferrer"
        className={`${baseClass} hover:opacity-80 transition-opacity cursor-pointer`}
        onClick={(e) => e.stopPropagation()}
      >
        {inner}
      </a>
    )
  }

  return (
    <span className={baseClass}>
      {inner}
    </span>
  )
}
