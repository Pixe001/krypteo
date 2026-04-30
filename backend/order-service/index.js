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
    info: { title: 'Order Service API', version: '1.0.0', description: 'Orchestrates the purchase Saga' },
  },
  apis: ['./index.js'],
};
const specs = swaggerJsdoc(swaggerOptions);
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(specs));

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

let channel;
let amqpConnected = false;

/**
 * @openapi
 * /health:
 *   get:
 *     summary: Health Check
 *     responses:
 *       200:
 *         description: Service status
 */
app.get('/health', async (req, res) => {
  try {
    const dbStatus = await pool.query('SELECT 1');
    res.json({
      status: 'UP',
      database: dbStatus ? 'CONNECTED' : 'DOWN',
      rabbitmq: amqpConnected ? 'CONNECTED' : 'DOWN'
    });
  } catch (err) {
    res.status(500).json({ status: 'DOWN', error: err.message });
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
      console.log(`[ORDER] Received Saga Event: ${type} - CorrelationID: ${correlationId}`);
      
      const client = await pool.connect();
      try {
        if (type === 'FUNDS_RESERVED') {
          await client.query('UPDATE orders SET status = $1 WHERE correlation_id = $2', ['FUNDS_RESERVED', correlationId]);
          channel.sendToQueue('catalog_commands', Buffer.from(JSON.stringify({ 
            type: 'RESERVE_STOCK', 
            payload: { symbol: payload.symbol, amount: payload.amountBtc }, 
            correlationId 
          })));
        } else if (type === 'FUNDS_RESERVATION_FAILED') {
          await client.query('UPDATE orders SET status = $1, error_message = $2 WHERE correlation_id = $3', ['FAILED', reason, correlationId]);
        } else if (type === 'STOCK_RESERVED') {
          await client.query('UPDATE orders SET status = $1 WHERE correlation_id = $2', ['STOCK_RESERVED', correlationId]);
          channel.sendToQueue('wallet_commands', Buffer.from(JSON.stringify({ 
            type: 'COMMIT_TRANSACTION', 
            payload: { userId: payload.userId, amountEur: payload.amountEur, amountBtc: payload.amountBtc }, 
            correlationId 
          })));
          channel.sendToQueue('catalog_commands', Buffer.from(JSON.stringify({ 
            type: 'COMMIT_STOCK', 
            payload: { symbol: payload.symbol, amount: payload.amountBtc }, 
            correlationId 
          })));
          await client.query('UPDATE orders SET status = $1 WHERE correlation_id = $2', ['COMPLETED', correlationId]);
        } else if (type === 'STOCK_RESERVATION_FAILED') {
          await client.query('UPDATE orders SET status = $1, error_message = $2 WHERE correlation_id = $3', ['COMPENSATING', reason, correlationId]);
          channel.sendToQueue('wallet_commands', Buffer.from(JSON.stringify({ 
            type: 'COMPENSATE_FUNDS', 
            payload: { userId: payload.userId, amount: payload.amountEur }, 
            correlationId 
          })));
          await client.query('UPDATE orders SET status = $1 WHERE correlation_id = $2', ['FAILED', correlationId]);
        }
      } catch (err) {
        console.error(err);
      } finally {
        client.release();
      }
      channel.ack(msg);
    });
  } catch (err) {
    amqpConnected = false;
    console.error("RabbitMQ connection error", err);
    setTimeout(connectRabbitMQ, 5000);
  }
}

/**
 * @openapi
 * /orders:
 *   get:
 *     summary: Get all orders
 *     responses:
 *       200:
 *         description: List of orders
 */
app.get('/orders', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM orders ORDER BY created_at DESC');
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * @openapi
 * /orders:
 *   post:
 *     summary: Place a new order
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               userId: { type: string }
 *               symbol: { type: string }
 *               amountEur: { type: number }
 *               amountBtc: { type: number }
 *     responses:
 *       202:
 *         description: Order accepted
 */
app.post('/orders', async (req, res) => {
  const correlationId = req.headers['x-correlation-id'] || uuidv4();
  const { userId, symbol, amountEur, amountBtc } = req.body;
  
  const client = await pool.connect();
  try {
    await client.query(
      'INSERT INTO orders (correlation_id, user_id, symbol, amount_eur, amount_btc, status) VALUES ($1, $2, $3, $4, $5, $6)',
      [correlationId, userId, symbol, amountEur, amountBtc, 'PENDING']
    );
    
    channel.sendToQueue('wallet_commands', Buffer.from(JSON.stringify({ 
      type: 'RESERVE_FUNDS', 
      payload: { userId, amount: amountEur, symbol, amountBtc }, 
      correlationId 
    })));
    
    res.status(202).json({ correlationId, status: 'PENDING' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

async function initDb() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS orders (
        correlation_id UUID PRIMARY KEY,
        user_id VARCHAR(50),
        symbol VARCHAR(10),
        amount_eur DECIMAL(18, 2),
        amount_btc DECIMAL(18, 8),
        status VARCHAR(20),
        error_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);
  } finally {
    client.release();
  }
}

app.listen(port, async () => {
  await initDb();
  await connectRabbitMQ();
  console.log(`Order service listening on port ${port}`);
});
