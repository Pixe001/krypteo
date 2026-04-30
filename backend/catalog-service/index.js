const express = require('express');
const { Pool } = require('pg');
const { Kafka } = require('kafkajs');
const WebSocket = require('ws');
const http = require('http');
const swaggerUi = require('swagger-ui-express');
const swaggerJsdoc = require('swagger-jsdoc');

const app = express();
app.use(express.json());
const port = 3000;
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Swagger
const swaggerOptions = { definition: { openapi: '3.0.0', info: { title: 'Catalog API', version: '1.0.0' } }, apis: ['./index.js'] };
const specs = swaggerJsdoc(swaggerOptions);
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(specs));

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const kafka = new Kafka({ clientId: 'catalog-service', brokers: [process.env.KAFKA_BROKERS] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'catalog-group' });

let kafkaConnected = false;
let dbConnected = false;
let currentBtcPrice = 50000;

app.get('/health', async (req, res) => {
  res.json({ status: dbConnected && kafkaConnected ? 'UP' : 'PARTIAL_DOWN', database: dbConnected, kafka: kafkaConnected, btcPrice: currentBtcPrice });
});

app.get('/price/BTC', (req, res) => res.json({ symbol: 'BTC', priceEur: currentBtcPrice }));

function startBinanceStream() {
  const binanceWs = new WebSocket('wss://stream.binance.com:9443/ws/btceur@ticker');
  binanceWs.on('message', (data) => {
    const ticker = JSON.parse(data);
    currentBtcPrice = parseFloat(ticker.c);
    const payload = JSON.stringify({ symbol: 'BTC', price: currentBtcPrice, time: Date.now() });
    wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(payload); });
  });
  binanceWs.on('close', () => setTimeout(startBinanceStream, 5000));
}
startBinanceStream();

async function startKafka() {
  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: 'catalog-commands', fromBeginning: true });
    kafkaConnected = true;
    console.log("[CATALOG] Kafka connected");

    await consumer.run({
      eachMessage: async ({ message }) => {
        const { type, payload, correlationId } = JSON.parse(message.value.toString());
        if (type === 'RESERVE_STOCK') {
          const client = await pool.connect();
          try {
            await client.query('BEGIN');
            const res = await client.query('SELECT stock FROM inventory WHERE symbol = $1 FOR UPDATE', [payload.symbol]);
            if (res.rows.length > 0 && res.rows[0].stock >= payload.amount) {
              await client.query('UPDATE inventory SET stock = stock - $1, reserved = reserved + $1 WHERE symbol = $2', [payload.amount, payload.symbol]);
              await client.query('COMMIT');
              await producer.send({ topic: 'saga-events', messages: [{ value: JSON.stringify({ type: 'STOCK_RESERVED', payload, correlationId }) }] });
            } else {
              await client.query('ROLLBACK');
              await producer.send({ topic: 'saga-events', messages: [{ value: JSON.stringify({ type: 'STOCK_RESERVATION_FAILED', payload, correlationId, reason: 'Out of stock' }) }] });
            }
          } catch (e) {
            await client.query('ROLLBACK');
            await producer.send({ topic: 'saga-events', messages: [{ value: JSON.stringify({ type: 'STOCK_RESERVATION_FAILED', payload, correlationId, reason: e.message }) }] });
          } finally { client.release(); }
        } else if (type === 'COMMIT_STOCK') {
          await pool.query('UPDATE inventory SET reserved = reserved - $1 WHERE symbol = $2', [payload.amount, payload.symbol]);
        } else if (type === 'COMPENSATE_STOCK') {
          await pool.query('UPDATE inventory SET stock = stock + $1, reserved = reserved - $1 WHERE symbol = $2', [payload.amount, payload.symbol]);
        }
      },
    });
  } catch (err) {
    kafkaConnected = false;
    setTimeout(startKafka, 5000);
  }
}

async function initDb() {
  try {
    const client = await pool.connect();
    try {
      await client.query(`CREATE TABLE IF NOT EXISTS inventory (symbol VARCHAR(10) PRIMARY KEY, stock DECIMAL(18, 8) DEFAULT 0, reserved DECIMAL(18, 8) DEFAULT 0, price_eur DECIMAL(18, 2) DEFAULT 0);
        INSERT INTO inventory (symbol, stock, price_eur) VALUES ('BTC', 10.0, 50000.00) ON CONFLICT DO NOTHING;`);
      dbConnected = true;
    } finally { client.release(); }
  } catch (err) { dbConnected = false; setTimeout(initDb, 5000); }
}

app.use((req, res) => res.status(404).json({ error: "Not found in Catalog" }));
server.listen(port, () => { initDb(); startKafka(); console.log(`Catalog service listening on port ${port}`); });
