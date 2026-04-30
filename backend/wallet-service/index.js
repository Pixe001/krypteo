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
    await channel.assertQueue('wallet_commands');
    await channel.assertQueue('saga_events');
    
    channel.consume('wallet_commands', async (msg) => {
      const { type, payload, correlationId } = JSON.parse(msg.content.toString());
      console.log(`[WALLET] Received ${type} - CorrelationID: ${correlationId}`);
      
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
          } catch (err) {
            await client.query('ROLLBACK');
            throw err;
          } finally {
            client.release();
          }
        } catch (err) {
          console.error(err);
          channel.sendToQueue('saga_events', Buffer.from(JSON.stringify({ type: 'FUNDS_RESERVATION_FAILED', payload, correlationId, reason: err.message })));
        }
      } else if (type === 'COMMIT_TRANSACTION') {
        const { userId, amountEur, amountBtc } = payload;
        const client = await pool.connect();
        try {
          await client.query('BEGIN');
          await client.query('UPDATE wallets SET reserved_eur = reserved_eur - $1, balance_btc = balance_btc + $2 WHERE user_id = $3', [amountEur, amountBtc, userId]);
          await client.query('COMMIT');
          channel.sendToQueue('saga_events', Buffer.from(JSON.stringify({ type: 'TRANSACTION_COMMITTED', payload, correlationId })));
        } catch (err) {
          await client.query('ROLLBACK');
          console.error(err);
        } finally {
          client.release();
        }
      } else if (type === 'COMPENSATE_FUNDS') {
        const { userId, amount } = payload;
        const client = await pool.connect();
        try {
          await client.query('UPDATE wallets SET balance_eur = balance_eur + $1, reserved_eur = reserved_eur - $1 WHERE user_id = $2', [amount, userId]);
          console.log(`[WALLET] Compensated funds for ${userId} - CorrelationID: ${correlationId}`);
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
      CREATE TABLE IF NOT EXISTS wallets (
        user_id VARCHAR(50) PRIMARY KEY,
        balance_eur DECIMAL(18, 2) DEFAULT 0,
        reserved_eur DECIMAL(18, 2) DEFAULT 0,
        balance_btc DECIMAL(18, 8) DEFAULT 0
      );
      INSERT INTO wallets (user_id, balance_eur) VALUES ('user1', 1000.00) ON CONFLICT DO NOTHING;
    `);
  } finally {
    client.release();
  }
}

app.listen(port, async () => {
  await initDb();
  await connectRabbitMQ();
  console.log(`Wallet service listening on port ${port}`);
});
