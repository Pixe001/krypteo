import { useState, useEffect } from 'react'
import './App.css'

const GATEWAY_URL = 'http://localhost:3010'
const USER_ID = 'user1'

function App() {
  const [wallet, setWallet] = useState({ balance_eur: 0, balance_btc: 0 })
  const [orders, setOrders] = useState([])
  const [amountEur, setAmountEur] = useState('')
  const [loading, setLoading] = useState(false)

  const fetchData = async () => {
    try {
      const [walletRes, ordersRes] = await Promise.all([
        fetch(`${GATEWAY_URL}/wallets/${USER_ID}`),
        fetch(`${GATEWAY_URL}/orders`)
      ])
      const walletData = await walletRes.json()
      const ordersData = await ordersRes.json()
      setWallet(walletData)
      setOrders(ordersData)
    } catch (err) {
      console.error('Error fetching data:', err)
    }
  }

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 3000)
    return () => clearInterval(interval)
  }, [])

  const handleBuy = async (e) => {
    e.preventDefault()
    if (!amountEur || loading) return

    setLoading(true)
    try {
      const priceBtc = 50000 // Mock price matching Catalog Service
      const amountBtc = parseFloat(amountEur) / priceBtc

      const response = await fetch(`${GATEWAY_URL}/orders`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          userId: USER_ID,
          symbol: 'BTC',
          amountEur: parseFloat(amountEur),
          amountBtc: amountBtc
        })
      })

      if (response.ok) {
        setAmountEur('')
        fetchData()
      }
    } catch (err) {
      console.error('Error placing order:', err)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="container">
      <header>
        <h1>Krypteo</h1>
        <p style={{ color: '#666' }}>Trading de crypto-monnaies fiable et transparent.</p>
      </header>

      <div className="wallet-grid">
        <div className="card">
          <div className="balance-item">
            <h3>Solde EUR</h3>
            <p>{parseFloat(wallet.balance_eur).toFixed(2)} €</p>
          </div>
        </div>
        <div className="card">
          <div className="balance-item">
            <h3>Solde BTC</h3>
            <p>{parseFloat(wallet.balance_btc).toFixed(8)} BTC</p>
          </div>
        </div>
      </div>

      <div className="card">
        <form onSubmit={handleBuy} className="form-group">
          <label>Acheter du Bitcoin</label>
          <input
            type="number"
            placeholder="Montant en EUR"
            value={amountEur}
            onChange={(e) => setAmountEur(e.target.value)}
            min="1"
          />
          <button type="submit" disabled={loading}>
            {loading ? 'Traitement...' : 'Placer l\'ordre'}
          </button>
        </form>
      </div>

      <div className="card">
        <h3>Historique des ordres</h3>
        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Paire</th>
              <th>Montant</th>
              <th>Statut</th>
              <th>Trace ID</th>
            </tr>
          </thead>
          <tbody>
            {orders.map((order) => (
              <tr key={order.correlation_id}>
                <td>{new Date(order.created_at).toLocaleTimeString()}</td>
                <td>{order.symbol} / EUR</td>
                <td>{parseFloat(order.amount_eur).toFixed(2)} €</td>
                <td>
                  <span className={`status status-${order.status.toLowerCase()}`}>
                    {order.status}
                  </span>
                </td>
                <td className="correlation-id">{order.correlation_id}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default App
