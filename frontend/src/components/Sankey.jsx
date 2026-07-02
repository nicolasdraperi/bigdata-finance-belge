import { useState } from 'react'

export default function Sankey({ financials }) {
  const years = financials?.years || []
  const [idx, setIdx] = useState(0)

  if (!years.length) return null

  const y = years[idx]
  const ca = y.chiffre_affaires || 0
  const achats = y.achats || 0
  const margeBrute = y.ratios?.marge_brute ?? (ca - achats)
  const net = y.resultat_net || 0

  if (!ca) {
    return (
      <div>
        <YearSelector years={years} idx={idx} setIdx={setIdx} />
        <p className="text-slate-500 text-sm">
          Chiffre d'affaires non publié, Sankey indisponible pour {y.year}.
        </p>
      </div>
    )
  }

  const maxVal = Math.max(ca, Math.abs(margeBrute), Math.abs(net), 1)
  const H = 320
  const W = 700
  const h = (v) => Math.max(4, (Math.abs(v) / maxVal) * (H - 90))

  const barW = 135
  const x1 = 25
  const x2 = (W - barW) / 2
  const x3 = W - barW - 25
  const yTop = 55

  const hCA = h(ca)
  const hMarge = h(margeBrute)
  const hNet = h(net)

  const netColor = net < 0 ? '#fb7185' : '#34d399'

  return (
    <div>
      <YearSelector years={years} idx={idx} setIdx={setIdx} />

      <div className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] p-4 overflow-x-auto">
        <svg viewBox={`0 0 ${W} ${H}`} className="w-full min-w-[620px] h-auto">
          <defs>
            <linearGradient id="flowBlue" x1="0" x2="1">
              <stop offset="0%" stopColor="#8b5cf6" />
              <stop offset="100%" stopColor="#06b6d4" />
            </linearGradient>

            <linearGradient id="flowNet" x1="0" x2="1">
              <stop offset="0%" stopColor="#06b6d4" />
              <stop offset="100%" stopColor={netColor} />
            </linearGradient>

            <filter id="glow">
              <feGaussianBlur stdDeviation="4" result="coloredBlur" />
              <feMerge>
                <feMergeNode in="coloredBlur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>

          <path
            d={flowPath(x1 + barW, yTop, hCA, x2, yTop, hMarge)}
            fill="url(#flowBlue)"
            opacity="0.25"
          />

          <path
            d={flowPath(x2 + barW, yTop, hMarge, x3, yTop, hNet)}
            fill="url(#flowNet)"
            opacity="0.25"
          />

          <Bar
            x={x1}
            y={yTop}
            w={barW}
            h={hCA}
            color="#8b5cf6"
            label="Chiffre d'affaires"
            value={ca}
          />

          <Bar
            x={x2}
            y={yTop}
            w={barW}
            h={hMarge}
            color="#06b6d4"
            label="Marge brute"
            value={margeBrute}
          />

          <Bar
            x={x3}
            y={yTop}
            w={barW}
            h={hNet}
            color={netColor}
            label="Résultat net"
            value={net}
          />
        </svg>
      </div>

      {net < 0 && (
        <p className="mt-3 text-center text-sm text-rose-400">
          ⚠️ Exercice déficitaire ({fmt(net)} €)
        </p>
      )}
    </div>
  )
}

function Bar({ x, y, w, h, color, label, value }) {
  return (
    <g>
      <rect x={x} y={y} width={w} height={h} fill={color} rx="8" opacity="0.9" />

      <text
        x={x + w / 2}
        y={y - 18}
        textAnchor="middle"
        fontSize="13"
        fontWeight="700"
        fill="#e2e8f0"
      >
        {label}
      </text>

      <text
        x={x + w / 2}
        y={y + h + 22}
        textAnchor="middle"
        fontSize="12"
        fill="#94a3b8"
      >
        {fmt(value)} €
      </text>
    </g>
  )
}

function flowPath(x1, y1, h1, x2, y2, h2) {
  const midX = (x1 + x2) / 2

  return `M ${x1} ${y1}
          C ${midX} ${y1}, ${midX} ${y2}, ${x2} ${y2}
          L ${x2} ${y2 + h2}
          C ${midX} ${y2 + h2}, ${midX} ${y1 + h1}, ${x1} ${y1 + h1}
          Z`
}

function YearSelector({ years, idx, setIdx }) {
  return (
    <div className="flex items-center gap-3 mb-3">
      <label className="text-sm text-slate-400">Exercice :</label>

      <select
        value={idx}
        onChange={(e) => setIdx(Number(e.target.value))}
        className="bg-white/10 border border-white/10 rounded-xl px-3 py-2 text-sm text-white
                   focus:outline-none focus:border-cyan-400/50 transition"
      >
        {years.map((y, i) => (
          <option key={y.year} value={i} className="bg-slate-900">
            {y.year}
          </option>
        ))}
      </select>
    </div>
  )
}

const fmt = (n) => n ? Math.round(n).toLocaleString('fr-FR') : '0'