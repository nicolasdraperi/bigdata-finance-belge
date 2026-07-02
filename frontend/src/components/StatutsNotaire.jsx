import { useState, useRef } from 'react'

const API = 'http://localhost:8000'

export default function StatutsNotaire({ num }) {
  const [statuts, setStatuts] = useState([])
  const [status, setStatus] = useState('idle')
  const [msg, setMsg] = useState('')
  const esRef = useRef(null)

  const charger = () => {
    setStatuts([])
    setStatus('streaming')
    setMsg('')

    const es = new EventSource(`${API}/enterprise/${num}/statuts-stream`)
    esRef.current = es

    es.addEventListener('status', (e) => {
      setMsg(JSON.parse(e.data).msg)
    })

    es.addEventListener('document', (e) => {
      const doc = JSON.parse(e.data)
      setStatuts((prev) => [...prev, doc])
    })

    es.addEventListener('done', () => {
      setStatus('done')
      es.close()
    })

    es.addEventListener('error', (e) => {
      try {
        setMsg(JSON.parse(e.data).msg)
      } catch {}

      setStatus('done')
      es.close()
    })
  }

  return (
    <div>
      {status === 'idle' && (
        <button
          onClick={charger}
          className="inline-flex items-center gap-2 px-4 py-2.5 rounded-xl
                     bg-gradient-to-r from-violet-500 to-cyan-500
                     text-white text-sm font-semibold shadow-lg shadow-violet-500/20
                     hover:scale-[1.03] hover:shadow-cyan-500/20 transition"
        >
          📜 Charger les statuts notariés
        </button>
      )}

      {status === 'streaming' && (
        <div className="flex items-center gap-3 text-slate-400 text-sm">
          <span className="spinner" />
          <span>{msg || 'Scraping en cours...'}</span>
        </div>
      )}

      {statuts.length > 0 && (
        <ul className="mt-4 space-y-3">
          {statuts.map((s, i) => (
            <li
              key={i}
              style={{ animationDelay: `${i * 40}ms` }}
              className="bg-white/5 border border-white/10 rounded-2xl p-4
                         hover:bg-white/10 hover:border-white/20 transition animate-fade-up"
            >
              <div className="font-semibold text-white">
                {s.documentTitle || 'Statut'}
              </div>

              <div className="text-sm text-slate-400 mt-1">
                Acte du {s.deedDate || '—'} · {s.organizationName || '—'}
              </div>

              <div className="mt-2">
                <span className="text-xs px-3 py-1 rounded-full border
                                 bg-cyan-400/10 text-cyan-300 border-cyan-400/30">
                  {s.documentStatus || 'Statut inconnu'}
                </span>
              </div>
            </li>
          ))}
        </ul>
      )}

      {status === 'done' && statuts.length === 0 && (
        <p className="text-slate-500 text-sm">
          {msg || 'Aucun statut disponible.'}
        </p>
      )}
    </div>
  )
}