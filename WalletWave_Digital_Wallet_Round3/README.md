# WalletWave — Digital Wallet & Payment Platform

A simple local web application for Round 3 (High-Level System Design).

## Features
- Add money and transfer between two demo wallets
- Wallet balance and transaction history REST APIs
- Download transaction receipt as CSV
- Fraud alert for high-value transfers
- Notification/event queue demonstration
- Cache demonstration and architecture panel
- SQLite persistence; safe atomic transfer updates
- Idempotency-Key support for transfer requests

## Run on Windows
1. Install Python 3.10+.
2. Open a terminal in this folder.
3. Create and activate a virtual environment (optional):
   ```bash
   python -m venv venv
   venv\Scripts\activate
   ```
4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
5. Start the app:
   ```bash
   python app.py
   ```
6. Open http://127.0.0.1:5000

Demo wallets:
- W101 — Teju — initial balance ₹2,500.00
- W202 — Arun — initial balance ₹1,500.00

## API endpoints
- GET `/api/health`
- GET `/api/architecture`
- GET `/api/wallets/W101/balance`
- POST `/api/wallets/W101/add-money` JSON: `{"amount": 100}`
- POST `/api/transfers` JSON: `{"senderWalletId":"W101","receiverWalletId":"W202","amount":100}`
- GET `/api/transactions`
- GET `/api/fraud-alerts`
- GET `/api/notifications`
- GET `/api/transactions/<transactionId>/download`

## How this maps to Round 3
- REST communication: Flask JSON endpoints
- Database: SQLite locally; PostgreSQL/MySQL recommended for production
- Caching: in-memory demo cache; Redis recommended for production
- Messaging: in-process event queue; Kafka/RabbitMQ recommended for production
- Scalability: documented load balancer, horizontal scaling, indexing and partitioning plan

## Important
This is an educational prototype, not a real banking/payment system. It does not connect to banks or payment gateways. For production, add proper authentication/authorization, durable message delivery (e.g. transactional outbox), external payment-provider integration, monitoring, rate limiting, secrets management and security testing.

