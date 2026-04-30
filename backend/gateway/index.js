const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');
const { v4: uuidv4 } = require('uuid');
const cors = require('cors');
const swaggerUi = require('swagger-ui-express');
const swaggerJsdoc = require('swagger-jsdoc');

const app = express();
const port = 3000;

app.use(cors());

// Swagger Setup (Aggregated for partners)
const swaggerOptions = {
  definition: {
    openapi: '3.0.0',
    info: { title: 'Krypteo API Gateway', version: '1.0.0', description: 'Unified entry point for Krypteo Microservices' },
  },
  apis: [], // We'll manually define paths or redirect
};
const specs = swaggerJsdoc(swaggerOptions);
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(specs));

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

// Proxy to Wallet Service
app.use('/wallets', createProxyMiddleware({
  target: 'http://wallet-service:3000',
  changeOrigin: true,
  onProxyReq: (proxyReq, req, res) => {
    proxyReq.setHeader('x-correlation-id', req.headers['x-correlation-id']);
  }
}));

// Proxy Swagger Docs of sub-services
app.use('/docs/orders', createProxyMiddleware({ target: 'http://order-service:3000', pathRewrite: { '^/docs/orders': '/api-docs' } }));
app.use('/docs/wallets', createProxyMiddleware({ target: 'http://wallet-service:3000', pathRewrite: { '^/docs/wallets': '/api-docs' } }));
app.use('/docs/catalog', createProxyMiddleware({ target: 'http://catalog-service:3000', pathRewrite: { '^/docs/catalog': '/api-docs' } }));

app.get('/health', (req, res) => res.json({ status: 'UP', gateway: 'OK' }));

app.listen(port, () => {
  console.log(`Gateway listening on port ${port}`);
});
