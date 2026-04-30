const express = require('express');
const { Pool } = require('pg');
const { Kafka } = require('kafkajs');
const swaggerUi = require('swagger-ui-express');
const swaggerJsdoc = require('swagger-jsdoc');

const app = express();
app.use(express.json());
const port = 3000;

// Swagger Setup
const swaggerOptions = {
  definition: { openapi: '3.0.0', info: { title: 'Wallet Service API', version: '1.0.0' } },
  apis: ['./index.js'],
};
const specs = swaggerJsdoc(swaggerOptions);
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(specs));

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const kafka = new Kafka({ clientId: 'wallet-service', brokers: [process.env.KAFKA_BROKERS] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'wallet-group' });

let kafkaConnected = false;
let dbConnected = false;

app.get('/health', async (req, res) => {
  res.json({ status: dbConnected && kafkaConnected ? 'UP' : 'PARTIAL_DOWN', database: dbConnected, kafka: kafkaConnected });
});

app.get('/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const result = await pool.query('SELECT balance_eur, balance_btc, reserved_eur FROM wallets WHERE user_id = $1', [userId]);
    if (result.rows.length > 0) res.json(result.rows[0]);
    else res.status(404).json({ error: 'Wallet not found' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

async function startKafka() {
  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: 'wallet-commands', fromBeginning: true });
    kafkaConnected = true;
    console.log("[WALLET] Kafka connected");

    await consumer.run({
      eachMessage: async ({ message }) => {
        const { type, payload, correlationId } = JSON.parse(message.value.toString());
        console.log(`[WALLET] Received ${type} - Trace: ${correlationId}`);
        
        if (type === 'RESERVE_FUNDS') {
          const client = await pool.connect();
          try {
            await client.query('BEGIN');
            const res = await client.query('SELECT balance_eur FROM wallets WHERE user_id = $1 FOR UPDATE', [payload.userId]);
            if (res.rows.length > 0 && res.rows[0].balance_eur >= payload.amount) {
              await client.query('UPDATE wallets SET balance_eur = balance_eur - $1, reserved_eur = reserved_eur + $1 WHERE user_id = $2', [payload.amount, payload.userId]);
              await client.query('COMMIT');
              await producer.send({ topic: 'saga-events', messages: [{ value: JSON.stringify({ type: 'FUNDS_RESERVED', payload, correlationId }) }] });
            } else {
              await client.query('ROLLBACK');
              await producer.send({ topic: 'saga-events', messages: [{ value: JSON.stringify({ type: 'FUNDS_RESERVATION_FAILED', payload, correlationId, reason: 'Insufficient funds' }) }] });
            }
          } catch (e) {
            await client.query('ROLLBACK');
            await producer.send({ topic: 'saga-events', messages: [{ value: JSON.stringify({ type: 'FUNDS_RESERVATION_FAILED', payload, correlationId, reason: e.message }) }] });
          } finally { client.release(); }
        } else if (type === 'COMMIT_TRANSACTION') {
          await pool.query('UPDATE wallets SET reserved_eur = reserved_eur - $1, balance_btc = balance_btc + $2 WHERE user_id = $3', [payload.amountEur, payload.amountBtc, payload.userId]);
        } else if (type === 'COMPENSATE_FUNDS') {
          await pool.query('UPDATE wallets SET balance_eur = balance_eur + $1, reserved_eur = reserved_eur - $1 WHERE user_id = $2', [payload.amount, payload.userId]);
        }
      },
    });
  } catch (err) {
    kafkaConnected = false;
    console.error("[WALLET] Kafka error, retrying...", err.message);
    setTimeout(startKafka, 5000);
  }
}

async function initDb() {
  try {
    const client = await pool.connect();
    try {
      await client.query(`CREATE TABLE IF NOT EXISTS wallets (user_id VARCHAR(50) PRIMARY KEY, balance_eur DECIMAL(18, 2) DEFAULT 0, reserved_eur DECIMAL(18, 2) DEFAULT 0, balance_btc DECIMAL(18, 8) DEFAULT 0);
        INSERT INTO wallets (user_id, balance_eur) VALUES ('user1', 1000.00) ON CONFLICT DO NOTHING;`);
      dbConnected = true;
    } finally { client.release(); }
  } catch (err) { dbConnected = false; setTimeout(initDb, 5000); }
}

app.use((req, res) => res.status(404).json({ error: "Not found in Wallet" }));

app.listen(port, () => {
  initDb();
  startKafka();
  console.log(`Wallet service listening on port ${port}`);
});
