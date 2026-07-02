import { useParams, Link } from 'react-router-dom'
import { useDispatch, useSelector } from 'react-redux'
import { useEffect } from 'react'
import { fetchEnterprise, fetchDirigeants } from '../store'
import Sankey from '../components/Sankey'
import StatutsNotaire from '../components/StatutsNotaire'

export default function Enterprise() {
  const { num } = useParams()
  const dispatch = useDispatch()
  const { data, dirigeants, loading, error } = useSelector((s) => s.enterprise)

  useEffect(() => {
    dispatch(fetchEnterprise(num))
    dispatch(fetchDirigeants(num))
  }, [num, dispatch])

  return (
    <div className="relative min-h-screen">
      {/* Fond */}
      <div className="fixed inset-0 -z-10 bg-slate-950">
        <div className="absolute top-[-10%] left-[-5%] w-[45rem] h-[45rem] rounded-full bg-violet-600/30 blur-[120px]" />
        <div className="absolute bottom-[-15%] right-[-10%] w-[40rem] h-[40rem] rounded-full bg-cyan-500/20 blur-[120px]" />
      </div>

      <div className="max-w-4xl mx-auto px-4 py-12">
        <Link to="/" className="inline-flex items-center gap-2 text-slate-400 hover:text-white transition mb-8">
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
          </svg>
          Retour à la recherche
        </Link>

        {loading && <p className="text-slate-400">Chargement...</p>}
        {error && <p className="text-rose-400">{error}</p>}

        {data && (
          <>
            {/* En-tête */}
            <div className="mb-8">
              <h1 className="text-4xl font-extrabold text-white tracking-tight">{data.nom}</h1>
              <div className="flex items-center gap-2 mt-3 flex-wrap">
                <span className="text-sm text-slate-400">{data.enterprise_number}</span>
                <span className="text-slate-600">·</span>
                <span className="text-sm text-slate-400">{data.forme_juridique}</span>
                <span className={`text-xs px-3 py-1 rounded-full font-medium border ${
                  data.status === 'Actif'
                    ? 'bg-emerald-400/10 text-emerald-300 border-emerald-400/30'
                    : 'bg-slate-400/10 text-slate-400 border-slate-400/20'
                }`}>{data.status}</span>
              </div>
            </div>

            {/* Informations */}
            <Card title="Informations">
              <dl className="space-y-3 text-sm">
                <Row label="Date de début" value={data.date_debut} />
                {data.adresse && (
                  <Row label="Adresse" value={`${data.adresse.rue} ${data.adresse.numero}, ${data.adresse.code_postal} ${data.adresse.commune}`} />
                )}
              </dl>
              <div className="mt-4">
                <div className="text-slate-400 text-sm mb-2">Activités principales</div>
                <div className="flex flex-wrap gap-2">
                  {data.activites.filter(a => a.classification === 'MAIN').slice(0, 6).map((a, i) => (
                    <span key={i} className="text-xs px-3 py-1.5 rounded-lg bg-white/5 border border-white/10 text-slate-300">
                      {a.label} <span className="text-slate-500">· {a.code}</span>
                    </span>
                  ))}
                </div>
              </div>
            </Card>

            {/* Ratios */}
            <Card title="Ratios financiers">
              {data.financials?.years ? (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-slate-400 border-b border-white/10">
                        <th className="text-left py-2 font-medium">Année</th>
                        <th className="text-right font-medium">CA</th>
                        <th className="text-right font-medium">Résultat net</th>
                        <th className="text-right font-medium">Marge nette</th>
                        <th className="text-right font-medium">ROE</th>
                        <th className="text-right font-medium">Endettement</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.financials.years.map((y) => (
                        <tr key={y.year} className="border-b border-white/5 hover:bg-white/5 transition">
                          <td className="py-3 font-semibold text-white">{y.year}</td>
                          <td className="text-right text-slate-300">{fmt(y.chiffre_affaires)}</td>
                          <td className={`text-right font-medium ${y.resultat_net < 0 ? 'text-rose-400' : 'text-emerald-400'}`}>{fmt(y.resultat_net)}</td>
                          <td className="text-right text-slate-300">{pct(y.ratios.marge_nette_pct)}</td>
                          <td className="text-right text-slate-300">{pct(y.ratios.roe_pct)}</td>
                          <td className="text-right text-slate-300">{pct(y.ratios.taux_endettement_pct)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : <Empty>Aucune donnée financière disponible</Empty>}
            </Card>

            {/* Sankey */}
            {data.financials?.years?.length > 0 && (
              <Card title="Compte de résultats">
                <Sankey financials={data.financials} />
              </Card>
            )}

            {/* Dirigeants */}
            <Card title="Dirigeants">
              {dirigeants.length ? (
                <ul className="space-y-2">
                  {dirigeants.map((d, i) => (
                    <li key={i} className="flex items-center justify-between py-2 border-b border-white/5 last:border-0">
                      <div>
                        <span className="text-white font-medium">{d.nom}</span>
                        <span className="text-slate-400 text-sm ml-2">{d.fonction}</span>
                      </div>
                      <span className="text-xs text-slate-500">{d.depuis}</span>
                    </li>
                  ))}
                </ul>
              ) : <Empty>Aucun dirigeant trouvé</Empty>}
            </Card>

            {/* Statuts notariés */}
            <Card title="Statuts notariés">
              <StatutsNotaire num={data.enterprise_number} />
            </Card>
          </>
        )}
      </div>
    </div>
  )
}

const fmt = (n) => n ? Math.round(n).toLocaleString('fr-FR') + ' €' : '—'
const pct = (n) => (n === null || n === undefined) ? '—' : (Math.abs(n) > 1000 ? 'n/a' : n + '%')

function Card({ title, children }) {
  return (
    <div className="bg-white/5 backdrop-blur-xl rounded-2xl border border-white/10 p-6 mb-5">
      <h2 className="text-lg font-semibold text-white mb-4">{title}</h2>
      {children}
    </div>
  )
}
function Row({ label, value }) {
  return (
    <div className="flex justify-between gap-4">
      <dt className="text-slate-400">{label}</dt>
      <dd className="text-slate-200 text-right">{value}</dd>
    </div>
  )
}
function Empty({ children }) {
  return <p className="text-slate-500 text-sm">{children}</p>
}