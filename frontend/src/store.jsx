import { configureStore, createSlice, createAsyncThunk } from '@reduxjs/toolkit'

const API = 'http://localhost:8000'

export const doSearch = createAsyncThunk('search/do', async ({ q, scope }) => {
  const r = await fetch(`${API}/search?q=${encodeURIComponent(q)}&scope=${scope}`)
  return (await r.json()).results
})

const searchSlice = createSlice({
  name: 'search',
  initialState: { query: '', results: [], loading: false, scope: 'hotels' },
  reducers: {
    setQuery: (s, a) => { s.query = a.payload },
    setScope: (s, a) => { s.scope = a.payload },
  },
  extraReducers: (b) => {
    b.addCase(doSearch.pending, (s) => { s.loading = true })
     .addCase(doSearch.fulfilled, (s, a) => { s.loading = false; s.results = a.payload })
     .addCase(doSearch.rejected, (s) => { s.loading = false })
  }
})

export const fetchEnterprise = createAsyncThunk('enterprise/fetch', async (num) => {
  const r = await fetch(`${API}/enterprise/${num}`)
  if (!r.ok) throw new Error('not found')
  return await r.json()
})

export const fetchDirigeants = createAsyncThunk('enterprise/dirigeants', async (num) => {
  const r = await fetch(`${API}/enterprise/${num}/dirigeants`)
  return (await r.json()).dirigeants
})
export const fetchLiens = createAsyncThunk('enterprise/liens', async (num) => {
  const r = await fetch(`${API}/enterprise/${num}/liens`)
  return (await r.json()).liens
})

const enterpriseSlice = createSlice({
  name: 'enterprise',
    initialState: {
    data: null, dirigeants: [], liens: [], loading: false, error: null,
    cache: {},
    history: [],
  },
  reducers: {
    loadFromCache: (s, a) => {
      const num = a.payload
      if (s.cache[num]) {
        s.data = s.cache[num].data
        s.dirigeants = s.cache[num].dirigeants || []
        s.liens = s.cache[num].liens || []
        s.loading = false
        s.error = null
      }
    },
  },
  extraReducers: (b) => {
    b.addCase(fetchEnterprise.pending, (s) => { s.loading = true; s.error = null; s.data = null })
     .addCase(fetchEnterprise.fulfilled, (s, a) => {
        s.loading = false
        s.data = a.payload
        const num = a.payload.enterprise_number
        s.cache[num] = { ...(s.cache[num] || {}), data: a.payload }
        s.history = [{ num, nom: a.payload.nom },
                     ...s.history.filter(h => h.num !== num)].slice(0, 8)
     })
     .addCase(fetchEnterprise.rejected, (s) => { s.loading = false; s.error = 'Entreprise introuvable' })
     .addCase(fetchDirigeants.fulfilled, (s, a) => {
        s.dirigeants = a.payload
        if (s.data) {
          const num = s.data.enterprise_number
          s.cache[num] = { ...(s.cache[num] || {}), dirigeants: a.payload }
        }
     })
      .addCase(fetchLiens.fulfilled, (s, a) => {
        s.liens = a.payload
        if (s.data) {
          const num = s.data.enterprise_number
          s.cache[num] = { ...(s.cache[num] || {}), liens: a.payload }
        }
     })
  }

})

export const { setQuery, setScope } = searchSlice.actions
export const { loadFromCache } = enterpriseSlice.actions
export const store = configureStore({
  reducer: {
    search: searchSlice.reducer,
    enterprise: enterpriseSlice.reducer,
  }
})