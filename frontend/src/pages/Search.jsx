import { useDispatch, useSelector } from 'react-redux'
import { useNavigate } from 'react-router-dom'
import { useRef } from 'react'
import { setQuery, doSearch } from '../store'

export default function Search() {
  const dispatch = useDispatch()
  const navigate = useNavigate()
  const { query, results, loading } = useSelector((s) => s.search)
  const timer = useRef(null)

  const onChange = (e) => {
    const q = e.target.value
    dispatch(setQuery(q))
    if (timer.current) clearTimeout(timer.current)
    if (q.trim().length >= 2) timer.current = setTimeout(() => dispatch(doSearch(q)), 300)
  }

  return (
    <div className="relative min-h-screen overflow-hidden">
      {/* Fond à blobs colorés */}
      <div className="fixed inset-0 -z-10 bg-slate-950">
        <div className="absolute top-[-10%] left-[-5%] w-[45rem] h-[45rem] rounded-full
                        bg-violet-600/40 blur-[120px] animate-pulse-slow" />
        <div className="absolute bottom-[-15%] right-[-10%] w-[40rem] h-[40rem] rounded-full
                        bg-fuchsia-500/30 blur-[120px]" />
        <div className="absolute top-[30%] right-[20%] w-[30rem] h-[30rem] rounded-full
                        bg-cyan-500/20 blur-[100px]" />
      </div>

      <div className="max-w-3xl mx-auto px-4 py-20">
        {/* Titre */}
        <div className="text-center mb-12">
          <h1 className="text-6xl md:text-7xl font-extrabold tracking-tight
                         bg-gradient-to-r from-violet-300 via-fuchsia-300 to-cyan-300
                         bg-clip-text text-transparent drop-shadow-2xl">
            BCE Hôtellerie
          </h1>
          <p className="mt-4 text-slate-300/80 text-lg font-light">
            Données financières des entreprises hôtelières belges
          </p>
        </div>

        {/* Barre de recherche en verre */}
        <div className="relative group">
          <div className="absolute -inset-0.5 bg-gradient-to-r from-violet-500 to-cyan-500
                          rounded-2xl blur opacity-30 group-focus-within:opacity-60 transition duration-300" />
          <div className="relative flex items-center">
            <svg className="absolute left-5 w-5 h-5 text-slate-400" fill="none"
                 viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                    d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
            <input
              value={query}
              onChange={onChange}
              placeholder="Rechercher par nom ou numéro BCE..."
              className="w-full pl-14 pr-5 py-5 text-lg text-white placeholder-slate-400
                         bg-white/10 backdrop-blur-xl rounded-2xl border border-white/20
                         focus:outline-none focus:border-white/40 transition"
            />
            {loading && <span className="spinner absolute right-5" />}
          </div>
        </div>

        {/* Résultats en cartes de verre */}
        <div className="mt-6 space-y-3">
          {results.map((r, i) => (
            <button
              key={r.enterprise_number}
              onClick={() => navigate(`/enterprise/${r.enterprise_number}`)}
              style={{ animationDelay: `${i * 40}ms` }}
              className="w-full text-left bg-white/10 backdrop-blur-xl rounded-2xl border border-white/10
                         p-5 hover:bg-white/20 hover:border-white/30 hover:scale-[1.02]
                         transition-all duration-200 animate-fade-up"
            >
              <div className="flex items-center justify-between gap-4">
                <div className="min-w-0">
                  <div className="font-semibold text-white truncate">{r.nom}</div>
                  <div className="text-sm text-slate-400 mt-1 truncate">
                    {r.forme} · {r.enterprise_number}
                  </div>
                </div>
                <span className={`shrink-0 text-xs px-3 py-1 rounded-full font-medium border ${
                  r.status === 'Actif'
                    ? 'bg-emerald-400/10 text-emerald-300 border-emerald-400/30'
                    : 'bg-slate-400/10 text-slate-400 border-slate-400/20'
                }`}>
                  {r.status}
                </span>
              </div>
            </button>
          ))}
          {query.length >= 2 && !loading && results.length === 0 && (
            <p className="text-center text-slate-400 py-10">Aucun résultat pour « {query} »</p>
          )}
        </div>
      </div>
    </div>
  )
}