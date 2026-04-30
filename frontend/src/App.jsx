import { useState, useEffect, useRef } from 'react'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import { TrendingUp, Wallet, Clock, AlertCircle } from 'lucide-react'
import './App.css'

const GATEWAY_URL = 'http://localhost:3010'
const WS_URL = 'ws://localhost:3010/catalog'
const USER_ID = 'user1'

function App() {
  const [wallet, setWallet] = useState({ balance_eur: 0, balance_btc: 0 })
  const [orders, setOrders] = useState([])
  const [btcPrice, setBtcPrice] = useState(0)
  const [priceHistory, setPriceHistory] = useState([])
  const [amountEur, setAmountEur] = useState('')
  const [loading, setLoading] = useState(false)
  const [message, setMessage] = useState(null)
  
  const ws = useRef(null)

  const fetchData = async () => {
    try {
      const [walletRes, ordersRes] = await Promise.all([
        fetch(`${GATEWAY_URL}/wallets/${USER_ID}`),
        fetch(`${GATEWAY_URL}/orders`)
      ])
      if (walletRes.ok) setWallet(await walletRes.json())
      if (ordersRes.ok) setOrders(await ordersRes.json())
    } catch (err) {
      console.error('Error fetching data:', err)
    }
  }

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 5000)

    // WebSocket for Real-time price
    ws.current = new WebSocket(WS_URL)
    ws.current.onmessage = (event) => {
      const data = JSON.parse(event.data)
      const newPrice = data.price
      setBtcPrice(newPrice)
      setPriceHistory(prev => {
        const updated = [...prev, { time: new Date().toLocaleTimeString(), price: newPrice }].slice(-60)
        return updated
      })
    }

    return () => {
      clearInterval(interval)
      if (ws.current) ws.current.close()
    }
  }, [])

  const handleBuy = async (e) => {
    e.preventDefault()
    if (!amountEur || loading || btcPrice === 0) return

    setLoading(true)
    setMessage({ type: 'info', text: 'Initiation de la Saga transactionnelle...' })
    try {
      const amountBtc = parseFloat(amountEur) / btcPrice
      const response = await fetch(`${GATEWAY_URL}/orders`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId: USER_ID, symbol: 'BTC', amountEur: parseFloat(amountEur), amountBtc })
      })

      if (response.ok) {
        setAmountEur('')
        setMessage({ type: 'success', text: 'Ordre accepté. Traitement asynchrone en cours...' })
      } else {
        const errorData = await response.json()
        setMessage({ type: 'error', text: `Échec: ${errorData.error}` })
      }
    } catch (err) {
      setMessage({ type: 'error', text: 'Erreur de connexion à la Gateway' })
    } finally {
      setLoading(false)
      setTimeout(() => setMessage(null), 5000)
    }
  }

  return (
    <div className="container">
      <header className="main-header">
        <div className="logo">
          <TrendingUp size={32} color="#4ade80" />
          <h1>Krypteo <span className="badge">LIVE</span></h1>
        </div>
        <div className="user-profile">
          <Wallet size={20} />
          <span>{USER_ID}</span>
        </div>
      </header>

      <section className="chart-section card">
        <div className="chart-header">
          <div>
            <h3>Bitcoin / Euro</h3>
            <p className="live-price">{btcPrice ? `${btcPrice.toLocaleString()} €` : 'Connexion au flux...'}</p>
          </div>
          <div className="indicator">
            <div className="dot pulse"></div>
            En direct de Binance
          </div>
        </div>
        <div className="chart-container">
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={priceHistory}>
              <XAxis dataKey="time" hide />
              <YAxis domain={['auto', 'auto']} hide />
              <Tooltip 
                contentStyle={{ background: '#1a1a1a', border: 'none', borderRadius: '8px', color: '#fff' }}
                itemStyle={{ color: '#4ade80' }}
              />
              <Line 
                type="monotone" 
                dataKey="price" 
                stroke="#4ade80" 
                strokeWidth={3} 
                dot={false} 
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </section>

      <div className="main-grid">
        <aside className="sidebar">
          <div className="card wallet-card">
            <h3>Portefeuille</h3>
            <div className="balance-row">
              <span className="label">EUR</span>
              <span className="value">{parseFloat(wallet.balance_eur).toFixed(2)} €</span>
            </div>
            <div className="balance-row">
              <span className="label">BTC</span>
              <span className="value">{parseFloat(wallet.balance_btc).toFixed(8)} BTC</span>
            </div>
          </div>

          <div className="card trade-card">
            <h3>Acheter du BTC</h3>
            <form onSubmit={handleBuy}>
              <div className="input-box">
                <input
                  type="number"
                  placeholder="Montant EUR"
                  value={amountEur}
                  onChange={(e) => setAmountEur(e.target.value)}
                />
              </div>
              <div className="estimate">
                Estimation: {amountEur && btcPrice ? (parseFloat(amountEur) / btcPrice).toFixed(8) : '0.00'} BTC
              </div>
              <button type="submit" disabled={loading || !btcPrice} className="buy-btn">
                {loading ? 'Saga...' : 'Acheter'}
              </button>
            </form>
            {message && (
              <div className={`message-toast ${message.type}`}>
                {message.type === 'error' ? <AlertCircle size={16} /> : <Clock size={16} />}
                {message.text}
              </div>
            )}
          </div>
        </aside>

        <main className="content">
          <div className="card history-card">
            <h3>Historique des Transactions</h3>
            <div className="table-wrapper">
              <table>
                <thead>
                  <tr>
                    <th>Heure</th>
                    <th>Montant</th>
                    <th>Statut</th>
                    <th>Trace ID</th>
                  </tr>
                </thead>
                <tbody>
                  {orders.map((order) => (
                    <tr key={order.correlation_id}>
                      <td>{new Date(order.created_at).toLocaleTimeString()}</td>
                      <td>{parseFloat(order.amount_eur).toFixed(2)} €</td>
                      <td>
                        <span className={`status-pill ${order.status.toLowerCase()}`}>
                          {order.status}
                        </span>
                      </td>
                      <td className="trace-cell">{order.correlation_id.substring(0, 8)}...</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </main>
      </div>
    </div>
  )
}

export default App
