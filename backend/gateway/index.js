const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');
const { v4: uuidv4 } = require('uuid');
const cors = require('cors');
const http = require('http');

const app = express();
const port = 3000;

app.use(cors());

// Logging Middleware
app.use((req, res, next) => {
  const correlationId = req.headers['x-correlation-id'] || uuidv4();
  req.headers['x-correlation-id'] = correlationId;
  res.setHeader('x-correlation-id', correlationId);
  console.log(`[GATEWAY] ${req.method} ${req.url} - Trace: ${correlationId}`);
  next();
});

// Proxy to Order Service
app.use('/orders', createProxyMiddleware({
  target: 'http://order-service:3000',
  changeOrigin: true,
  pathRewrite: { '^/orders': '' },
  on: {
    proxyReq: (proxyReq, req) => proxyReq.setHeader('x-correlation-id', req.headers['x-correlation-id'])
  }
}));

// Proxy to Wallet Service
app.use('/wallets', createProxyMiddleware({
  target: 'http://wallet-service:3000',
  changeOrigin: true,
  pathRewrite: { '^/wallets': '' },
  on: {
    proxyReq: (proxyReq, req) => proxyReq.setHeader('x-correlation-id', req.headers['x-correlation-id'])
  }
}));

// Proxy to Catalog Service (Supports WebSockets)
const catalogProxy = createProxyMiddleware({
  target: 'http://catalog-service:3000',
  changeOrigin: true,
  ws: true, // Enable WebSocket proxying
  pathRewrite: { '^/catalog': '' },
  on: {
    proxyReq: (proxyReq, req) => proxyReq.setHeader('x-correlation-id', req.headers['x-correlation-id'])
  }
});
app.use('/catalog', catalogProxy);

app.get('/health', (req, res) => res.json({ status: 'UP', gateway: 'OK' }));
app.use((req, res) => res.status(404).json({ error: `Path ${req.url} not found on Gateway` }));

const server = http.createServer(app);

// Handle WebSocket upgrades for the Gateway
server.on('upgrade', (req, socket, head) => {
  if (req.url.startsWith('/catalog')) {
    catalogProxy.upgrade(req, socket, head);
  }
});

server.listen(port, () => {
  console.log(`Gateway listening on port ${port} (WS support enabled)`);
});
