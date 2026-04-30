const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());
const port = 3000;

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

let channel;
async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    channel = await connection.createChannel();
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
          // Final Step: Commit both
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
          // Compensation: Release funds
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
    console.error("RabbitMQ connection error", err);
    setTimeout(connectRabbitMQ, 5000);
  }
}

app.post('/orders', async (req, res) => {
  const correlationId = req.headers['x-correlation-id'] || uuidv4();
  const { userId, symbol, amountEur, amountBtc } = req.body;
  
  const client = await pool.connect();
  try {
    await client.query(
      'INSERT INTO orders (correlation_id, user_id, symbol, amount_eur, amount_btc, status) VALUES ($1, $2, $3, $4, $5, $6)',
      [correlationId, userId, symbol, amountEur, amountBtc, 'PENDING']
    );
    
    // Start Saga
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
