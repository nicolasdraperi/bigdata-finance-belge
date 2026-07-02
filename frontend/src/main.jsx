import React from 'react'
import ReactDOM from 'react-dom/client'
import { Provider } from 'react-redux'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { store } from './store'
import Search from './pages/Search'
import Enterprise from './pages/Enterprise'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <Provider store={store}>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Search />} />
          <Route path="/enterprise/:num" element={<Enterprise />} />
        </Routes>
      </BrowserRouter>
    </Provider>
  </React.StrictMode>
)