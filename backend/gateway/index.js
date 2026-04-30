const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');
const { v4: uuidv4 } = require('uuid');

const app = express();
const port = 3000;

// Middleware for Correlation ID and Logging
app.use((req, res, next) => {
  const correlationId = req.headers['x-correlation-id'] || uuidv4();
  req.headers['x-correlation-id'] = correlationId;
  res.setHeader('x-correlation-id', correlationId);
  console.log(`[GATEWAY] ${req.method} ${req.url} - CorrelationID: ${correlationId}`);
  next();
});

// Proxy to Order Service
app.use('/orders', createProxyMiddleware({
  target: 'http://order-service:3000',
  changeOrigin: true,
  onProxyReq: (proxyReq, req, res) => {
    proxyReq.setHeader('x-correlation-id', req.headers['x-correlation-id']);
  }
}));

app.get('/health', (req, res) => res.send('Gateway is healthy'));

app.listen(port, () => {
  console.log(`Gateway listening on port ${port}`);
});
