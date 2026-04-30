const express = require('express');
const { Pool } = require('pg');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');
const swaggerUi = require('swagger-ui-express');
const swaggerJsdoc = require('swagger-jsdoc');

const app = express();
app.use(express.json());
const port = 3000;

// Swagger
const swaggerOptions = { definition: { openapi: '3.0.0', info: { title: 'Order API', version: '1.0.0' } }, apis: ['./index.js'] };
const specs = swaggerJsdoc(swaggerOptions);
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(specs));

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const kafka = new Kafka({ clientId: 'order-service', brokers: [process.env.KAFKA_BROKERS] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'order-group' });

let kafkaConnected = false;
let dbConnected = false;

app.get('/health', async (req, res) => {
  res.json({ status: dbConnected && kafkaConnected ? 'UP' : 'PARTIAL_DOWN', database: dbConnected, kafka: kafkaConnected });
});

app.get('/', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM orders ORDER BY created_at DESC');
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/', async (req, res) => {
  const correlationId = req.headers['x-correlation-id'] || uuidv4();
  const { userId, symbol, amountEur, amountBtc } = req.body;
  try {
    await pool.query('INSERT INTO orders (correlation_id, user_id, symbol, amount_eur, amount_btc, status) VALUES ($1, $2, $3, $4, $5, $6)', [correlationId, userId, symbol, amountEur, amountBtc, 'PENDING']);
    await producer.send({ topic: 'wallet-commands', messages: [{ value: JSON.stringify({ type: 'RESERVE_FUNDS', payload: { userId, amount: amountEur, symbol, amountBtc }, correlationId }) }] });
    res.status(202).json({ correlationId, status: 'PENDING' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

async function startKafka() {
  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: 'saga-events', fromBeginning: true });
    kafkaConnected = true;
    console.log("[ORDER] Kafka connected");

    await consumer.run({
      eachMessage: async ({ message }) => {
        const { type, payload, correlationId, reason } = JSON.parse(message.value.toString());
        try {
          if (type === 'FUNDS_RESERVED') {
            await pool.query('UPDATE orders SET status = $1 WHERE correlation_id = $2', ['FUNDS_RESERVED', correlationId]);
            await producer.send({ topic: 'catalog-commands', messages: [{ value: JSON.stringify({ type: 'RESERVE_STOCK', payload: { symbol: payload.symbol, amount: payload.amountBtc }, correlationId }) }] });
          } else if (type === 'FUNDS_RESERVATION_FAILED') {
            await pool.query('UPDATE orders SET status = $1, error_message = $2 WHERE correlation_id = $3', ['FAILED', reason, correlationId]);
          } else if (type === 'STOCK_RESERVED') {
            await pool.query('UPDATE orders SET status = $1 WHERE correlation_id = $2', ['STOCK_RESERVED', correlationId]);
            await producer.send({ topic: 'wallet-commands', messages: [{ value: JSON.stringify({ type: 'COMMIT_TRANSACTION', payload, correlationId }) }] });
            await producer.send({ topic: 'catalog-commands', messages: [{ value: JSON.stringify({ type: 'COMMIT_STOCK', payload, correlationId }) }] });
            await pool.query('UPDATE orders SET status = $1 WHERE correlation_id = $2', ['COMPLETED', correlationId]);
          } else if (type === 'STOCK_RESERVATION_FAILED') {
            await pool.query('UPDATE orders SET status = $1, error_message = $2 WHERE correlation_id = $3', ['FAILED', reason, correlationId]);
            await producer.send({ topic: 'wallet-commands', messages: [{ value: JSON.stringify({ type: 'COMPENSATE_FUNDS', payload: { userId: payload.userId, amount: payload.amountEur }, correlationId }) }] });
          }
        } catch (e) { console.error(e); }
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
      await client.query(`CREATE TABLE IF NOT EXISTS orders (correlation_id UUID PRIMARY KEY, user_id VARCHAR(50), symbol VARCHAR(10), amount_eur DECIMAL(18, 2), amount_btc DECIMAL(18, 8), status VARCHAR(20), error_message TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);`);
      dbConnected = true;
    } finally { client.release(); }
  } catch (err) { dbConnected = false; setTimeout(initDb, 5000); }
}

app.use((req, res) => res.status(404).json({ error: "Not found in Order" }));
app.listen(port, () => { initDb(); startKafka(); console.log(`Order service listening on port ${port}`); });
