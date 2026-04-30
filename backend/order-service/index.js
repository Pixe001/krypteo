const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');
const swaggerUi = require('swagger-ui-express');
const swaggerJsdoc = require('swagger-jsdoc');

const app = express();
app.use(express.json());
const port = 3000;

// Swagger Setup
const swaggerOptions = {
  definition: {
    openapi: '3.0.0',
    info: { title: 'Order Service API', version: '1.0.0' },
  },
  apis: ['./index.js'],
};
const specs = swaggerJsdoc(swaggerOptions);
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(specs));

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

let channel;
let amqpConnected = false;
let dbConnected = false;

app.get('/health', async (req, res) => {
  res.json({ status: dbConnected && amqpConnected ? 'UP' : 'PARTIAL_DOWN', database: dbConnected, rabbitmq: amqpConnected });
});

/**
 * @openapi
 * /:
 *   get:
 *     summary: Get all orders
 *     responses:
 *       200:
 *         description: Orders list
 */
app.get('/', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM orders ORDER BY created_at DESC');
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * @openapi
 * /:
 *   post:
 *     summary: Place order
 *     responses:
 *       202:
 *         description: Accepted
 */
app.post('/', async (req, res) => {
  const correlationId = req.headers['x-correlation-id'] || uuidv4();
  const { userId, symbol, amountEur, amountBtc } = req.body;
  try {
    await pool.query(
      'INSERT INTO orders (correlation_id, user_id, symbol, amount_eur, amount_btc, status) VALUES ($1, $2, $3, $4, $5, $6)',
      [correlationId, userId, symbol, amountEur, amountBtc, 'PENDING']
    );
    channel.sendToQueue('wallet_commands', Buffer.from(JSON.stringify({ type: 'RESERVE_FUNDS', payload: { userId, amount: amountEur, symbol, amountBtc }, correlationId })));
    res.status(202).json({ correlationId, status: 'PENDING' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    channel = await connection.createChannel();
    amqpConnected = true;
    await channel.assertQueue('wallet_commands');
    await channel.assertQueue('catalog_commands');
    await channel.assertQueue('saga_events');
    channel.consume('saga_events', async (msg) => {
      const { type, payload, correlationId, reason } = JSON.parse(msg.content.toString());
      try {
        if (type === 'FUNDS_RESERVED') {
          await pool.query('UPDATE orders SET status = $1 WHERE correlation_id = $2', ['FUNDS_RESERVED', correlationId]);
          channel.sendToQueue('catalog_commands', Buffer.from(JSON.stringify({ type: 'RESERVE_STOCK', payload: { symbol: payload.symbol, amount: payload.amountBtc }, correlationId })));
        } else if (type === 'FUNDS_RESERVATION_FAILED') {
          await pool.query('UPDATE orders SET status = $1, error_message = $2 WHERE correlation_id = $3', ['FAILED', reason, correlationId]);
        } else if (type === 'STOCK_RESERVED') {
          await pool.query('UPDATE orders SET status = $1 WHERE correlation_id = $2', ['STOCK_RESERVED', correlationId]);
          channel.sendToQueue('wallet_commands', Buffer.from(JSON.stringify({ type: 'COMMIT_TRANSACTION', payload: { userId: payload.userId, amountEur: payload.amountEur, amountBtc: payload.amountBtc }, correlationId })));
          channel.sendToQueue('catalog_commands', Buffer.from(JSON.stringify({ type: 'COMMIT_STOCK', payload: { symbol: payload.symbol, amount: payload.amountBtc }, correlationId })));
          await pool.query('UPDATE orders SET status = $1 WHERE correlation_id = $2', ['COMPLETED', correlationId]);
        } else if (type === 'STOCK_RESERVATION_FAILED') {
          await pool.query('UPDATE orders SET status = $1, error_message = $2 WHERE correlation_id = $3', ['COMPENSATING', reason, correlationId]);
          channel.sendToQueue('wallet_commands', Buffer.from(JSON.stringify({ type: 'COMPENSATE_FUNDS', payload: { userId: payload.userId, amount: payload.amountEur }, correlationId })));
          await pool.query('UPDATE orders SET status = $1 WHERE correlation_id = $2', ['FAILED', correlationId]);
        }
      } catch (err) { console.error(err); }
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
      await client.query(`CREATE TABLE IF NOT EXISTS orders (correlation_id UUID PRIMARY KEY, user_id VARCHAR(50), symbol VARCHAR(10), amount_eur DECIMAL(18, 2), amount_btc DECIMAL(18, 8), status VARCHAR(20), error_message TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);`);
      dbConnected = true;
    } finally { client.release(); }
  } catch (err) {
    dbConnected = false;
    setTimeout(initDb, 5000);
  }
}

app.use((req, res) => res.status(404).json({ error: `Path ${req.url} not found in Order Service` }));

app.listen(port, () => {
  initDb();
  connectRabbitMQ();
  console.log(`Order service listening on port ${port}`);
});
