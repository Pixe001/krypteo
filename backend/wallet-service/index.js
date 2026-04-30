const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const swaggerUi = require('swagger-ui-express');
const swaggerJsdoc = require('swagger-jsdoc');

const app = express();
app.use(express.json());
const port = 3000;

// Swagger Setup
const swaggerOptions = {
  definition: {
    openapi: '3.0.0',
    info: { title: 'Wallet Service API', version: '1.0.0', description: 'Handles user balances' },
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
 * /{userId}:
 *   get:
 *     summary: Get user wallet
 *     responses:
 *       200:
 *         description: Wallet info
 */
app.get('/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const result = await pool.query('SELECT balance_eur, balance_btc, reserved_eur FROM wallets WHERE user_id = $1', [userId]);
    if (result.rows.length > 0) {
      res.json(result.rows[0]);
    } else {
      res.status(404).json({ error: 'Wallet not found' });
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    channel = await connection.createChannel();
    amqpConnected = true;
    console.log("[WALLET] Connected to RabbitMQ");
    await channel.assertQueue('wallet_commands');
    await channel.assertQueue('saga_events');
    
    channel.consume('wallet_commands', async (msg) => {
      const { type, payload, correlationId } = JSON.parse(msg.content.toString());
      if (type === 'RESERVE_FUNDS') {
        try {
          const { userId, amount } = payload;
          const client = await pool.connect();
          try {
            await client.query('BEGIN');
            const res = await client.query('SELECT balance_eur FROM wallets WHERE user_id = $1 FOR UPDATE', [userId]);
            if (res.rows.length > 0 && res.rows[0].balance_eur >= amount) {
              await client.query('UPDATE wallets SET balance_eur = balance_eur - $1, reserved_eur = reserved_eur + $1 WHERE user_id = $2', [amount, userId]);
              await client.query('COMMIT');
              channel.sendToQueue('saga_events', Buffer.from(JSON.stringify({ type: 'FUNDS_RESERVED', payload, correlationId })));
            } else {
              await client.query('ROLLBACK');
              channel.sendToQueue('saga_events', Buffer.from(JSON.stringify({ type: 'FUNDS_RESERVATION_FAILED', payload, correlationId, reason: 'Insufficient funds' })));
            }
          } finally { client.release(); }
        } catch (err) {
          channel.sendToQueue('saga_events', Buffer.from(JSON.stringify({ type: 'FUNDS_RESERVATION_FAILED', payload, correlationId, reason: err.message })));
        }
      } else if (type === 'COMMIT_TRANSACTION') {
        const { userId, amountEur, amountBtc } = payload;
        await pool.query('UPDATE wallets SET reserved_eur = reserved_eur - $1, balance_btc = balance_btc + $2 WHERE user_id = $3', [amountEur, amountBtc, userId]);
      } else if (type === 'COMPENSATE_FUNDS') {
        const { userId, amount } = payload;
        await pool.query('UPDATE wallets SET balance_eur = balance_eur + $1, reserved_eur = reserved_eur - $1 WHERE user_id = $2', [amount, userId]);
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
      await client.query(`
        CREATE TABLE IF NOT EXISTS wallets (
          user_id VARCHAR(50) PRIMARY KEY,
          balance_eur DECIMAL(18, 2) DEFAULT 0,
          reserved_eur DECIMAL(18, 2) DEFAULT 0,
          balance_btc DECIMAL(18, 8) DEFAULT 0
        );
        INSERT INTO wallets (user_id, balance_eur) VALUES ('user1', 1000.00) ON CONFLICT DO NOTHING;
      `);
      dbConnected = true;
    } finally { client.release(); }
  } catch (err) {
    dbConnected = false;
    setTimeout(initDb, 5000);
  }
}

// 404 Handler - MUST BE JSON
app.use((req, res) => res.status(404).json({ error: `Path ${req.url} not found in Wallet Service` }));

app.listen(port, () => {
  initDb();
  connectRabbitMQ();
  console.log(`Wallet service listening on port ${port}`);
});
