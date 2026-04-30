const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const axios = require('axios');
const WebSocket = require('ws');
const http = require('http');
const swaggerUi = require('swagger-ui-express');
const swaggerJsdoc = require('swagger-jsdoc');

const app = express();
app.use(express.json());
const port = 3000;

// Create HTTP server for both Express and WebSockets
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Swagger Setup
const swaggerOptions = {
  definition: {
    openapi: '3.0.0',
    info: { title: 'Catalog Service API', version: '1.0.0', description: 'Handles assets inventory and real-time prices' },
  },
  apis: ['./index.js'],
};
const specs = swaggerJsdoc(swaggerOptions);
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(specs));

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

let channel;
let amqpConnected = false;
let dbConnected = false;
let currentBtcPrice = 50000;

// WebSocket logic: Stream from Binance and broadcast to local clients
function startBinanceStream() {
  const binanceWs = new WebSocket('wss://stream.binance.com:9443/ws/btceur@ticker');

  binanceWs.on('message', (data) => {
    const ticker = JSON.parse(data);
    currentBtcPrice = parseFloat(ticker.c); // 'c' is the last price
    
    // Broadcast to all connected frontend clients
    const payload = JSON.stringify({ symbol: 'BTC', price: currentBtcPrice, time: Date.now() });
    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(payload);
      }
    });
  });

  binanceWs.on('error', (err) => {
    console.error("[CATALOG] Binance WS error:", err.message);
    setTimeout(startBinanceStream, 5000);
  });

  binanceWs.on('close', () => {
    console.log("[CATALOG] Binance WS closed, reconnecting...");
    setTimeout(startBinanceStream, 5000);
  });
}

startBinanceStream();

app.get('/health', async (req, res) => {
  res.json({
    status: dbConnected && amqpConnected ? 'UP' : 'PARTIAL_DOWN',
    database: dbConnected ? 'CONNECTED' : 'DOWN',
    rabbitmq: amqpConnected ? 'CONNECTED' : 'DOWN',
    btcPrice: currentBtcPrice
  });
});

app.get('/price/BTC', (req, res) => {
  res.json({ symbol: 'BTC', priceEur: currentBtcPrice });
});

async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    channel = await connection.createChannel();
    amqpConnected = true;
    console.log("[CATALOG] Connected to RabbitMQ");
    await channel.assertQueue('catalog_commands');
    await channel.assertQueue('saga_events');
    
    channel.consume('catalog_commands', async (msg) => {
      const { type, payload, correlationId } = JSON.parse(msg.content.toString());
      if (type === 'RESERVE_STOCK') {
        const { symbol, amount } = payload;
        const client = await pool.connect();
        try {
          await client.query('BEGIN');
          const res = await client.query('SELECT stock FROM inventory WHERE symbol = $1 FOR UPDATE', [symbol]);
          if (res.rows.length > 0 && res.rows[0].stock >= amount) {
            await client.query('UPDATE inventory SET stock = stock - $1, reserved = reserved + $1 WHERE symbol = $2', [amount, symbol]);
            await client.query('COMMIT');
            channel.sendToQueue('saga_events', Buffer.from(JSON.stringify({ type: 'STOCK_RESERVED', payload, correlationId })));
          } else {
            await client.query('ROLLBACK');
            channel.sendToQueue('saga_events', Buffer.from(JSON.stringify({ type: 'STOCK_RESERVATION_FAILED', payload, correlationId, reason: 'Out of stock' })));
          }
        } finally { client.release(); }
      } else if (type === 'COMMIT_STOCK') {
        const { symbol, amount } = payload;
        await pool.query('UPDATE inventory SET reserved = reserved - $1 WHERE symbol = $2', [amount, symbol]);
      } else if (type === 'COMPENSATE_STOCK') {
        const { symbol, amount } = payload;
        await pool.query('UPDATE inventory SET stock = stock + $1, reserved = reserved - $1 WHERE symbol = $2', [amount, symbol]);
      }
      channel.ack(msg);
    });
  } catch (err) {
    amqpConnected = false;
    setTimeout(connectRabbitMQ, 5000);
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
  } catch (err) {
    dbConnected = false;
    setTimeout(initDb, 5000);
  }
}

app.use((req, res) => res.status(404).json({ error: "Not found in Catalog" }));

server.listen(port, () => {
  initDb();
  connectRabbitMQ();
  console.log(`Catalog service with WebSocket listening on port ${port}`);
});
