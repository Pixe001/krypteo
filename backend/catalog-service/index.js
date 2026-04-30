const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');

const app = express();
app.use(express.json());
const port = 3000;

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

let channel;
async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    channel = await connection.createChannel();
    await channel.assertQueue('catalog_commands');
    await channel.assertQueue('saga_events');
    
    channel.consume('catalog_commands', async (msg) => {
      const { type, payload, correlationId } = JSON.parse(msg.content.toString());
      console.log(`[CATALOG] Received ${type} - CorrelationID: ${correlationId}`);
      
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
        } catch (err) {
          await client.query('ROLLBACK');
          channel.sendToQueue('saga_events', Buffer.from(JSON.stringify({ type: 'STOCK_RESERVATION_FAILED', payload, correlationId, reason: err.message })));
        } finally {
          client.release();
        }
      } else if (type === 'COMMIT_STOCK') {
        const { symbol, amount } = payload;
        const client = await pool.connect();
        try {
          await client.query('UPDATE inventory SET reserved = reserved - $1 WHERE symbol = $2', [amount, symbol]);
        } finally {
          client.release();
        }
      } else if (type === 'COMPENSATE_STOCK') {
        const { symbol, amount } = payload;
        const client = await pool.connect();
        try {
          await client.query('UPDATE inventory SET stock = stock + $1, reserved = reserved - $1 WHERE symbol = $2', [amount, symbol]);
          console.log(`[CATALOG] Compensated stock for ${symbol} - CorrelationID: ${correlationId}`);
        } finally {
          client.release();
        }
      }
      channel.ack(msg);
    });
  } catch (err) {
    console.error("RabbitMQ connection error", err);
    setTimeout(connectRabbitMQ, 5000);
  }
}

async function initDb() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS inventory (
        symbol VARCHAR(10) PRIMARY KEY,
        stock DECIMAL(18, 8) DEFAULT 0,
        reserved DECIMAL(18, 8) DEFAULT 0,
        price_eur DECIMAL(18, 2) DEFAULT 0
      );
      INSERT INTO inventory (symbol, stock, price_eur) VALUES ('BTC', 10.0, 50000.00) ON CONFLICT DO NOTHING;
    `);
  } finally {
    client.release();
  }
}

app.listen(port, async () => {
  await initDb();
  await connectRabbitMQ();
  console.log(`Catalog service listening on port ${port}`);
});
