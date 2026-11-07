import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Finder — Arbitrage Monitor',
  description: 'Real-time DEX/CEX arbitrage spread monitor',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en" className="dark">
      <body className="bg-[#0a0e17] text-slate-200 antialiased">{children}</body>
    </html>
  )
}
