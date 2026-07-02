import { configureStore, createSlice, createAsyncThunk } from '@reduxjs/toolkit'

const API = 'http://localhost:8000'

export const doSearch = createAsyncThunk('search/do', async (q) => {
  const r = await fetch(`${API}/search?q=${encodeURIComponent(q)}`)
  return (await r.json()).results
})

const searchSlice = createSlice({
  name: 'search',
  initialState: { query: '', results: [], loading: false },
  reducers: {
    setQuery: (s, a) => { s.query = a.payload },
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

const enterpriseSlice = createSlice({
  name: 'enterprise',
  initialState: { data: null, dirigeants: [], loading: false, error: null },
  reducers: {},
  extraReducers: (b) => {
    b.addCase(fetchEnterprise.pending, (s) => { s.loading = true; s.error = null; s.data = null })
     .addCase(fetchEnterprise.fulfilled, (s, a) => { s.loading = false; s.data = a.payload })
     .addCase(fetchEnterprise.rejected, (s) => { s.loading = false; s.error = 'Entreprise introuvable' })
     .addCase(fetchDirigeants.fulfilled, (s, a) => { s.dirigeants = a.payload })
  }
})

export const { setQuery } = searchSlice.actions
export const store = configureStore({
  reducer: {
    search: searchSlice.reducer,
    enterprise: enterpriseSlice.reducer,
  }
})