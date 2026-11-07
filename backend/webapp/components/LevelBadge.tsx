'use client'

import { getLevelLabel, getLevelStyle } from '@/lib/utils'
import type { SpreadLevel } from '@/lib/types'

interface Props {
  level: SpreadLevel
}

export default function LevelBadge({ level }: Props) {
  return (
    <span
      className={`inline-flex items-center rounded border text-xs font-semibold px-1.5 py-0.5 tracking-wide
        ${getLevelStyle(level)}`}
    >
      {getLevelLabel(level)}
    </span>
  )
}
