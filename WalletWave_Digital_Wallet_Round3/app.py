from flask import Flask, request, jsonify, render_template, Response
import sqlite3, os, csv, io, uuid, threading, queue
from datetime import datetime
from decimal import Decimal, InvalidOperation

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(__file__), "wallet.db")

# Demo-only cache and message queue. Replace with Redis and Kafka/RabbitMQ in production.
balance_cache = {}
cache_lock = threading.Lock()
event_queue = queue.Queue()
notifications = []

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    with get_db() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY, name TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS wallets (
            wallet_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL UNIQUE,
            balance_cents INTEGER NOT NULL DEFAULT 0 CHECK(balance_cents >= 0),
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        );
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY,
            sender_wallet TEXT,
            receiver_wallet TEXT,
            amount_cents INTEGER NOT NULL,
            transaction_type TEXT NOT NULL,
            status TEXT NOT NULL,
            message TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fraud_alerts (
            alert_id TEXT PRIMARY KEY,
            transaction_id TEXT NOT NULL,
            risk_score INTEGER NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """)
        for uid, name, wid, balance in [
            ("U101", "Teju", "W101", 250000),
            ("U202", "Arun", "W202", 150000),
        ]:
            db.execute("INSERT OR IGNORE INTO users(user_id,name) VALUES (?,?)", (uid, name))
            db.execute("INSERT OR IGNORE INTO wallets(wallet_id,user_id,balance_cents) VALUES (?,?,?)",
                       (wid, uid, balance))

def money(cents):
    return f"{cents / 100:.2f}"

def parse_amount(value):
    try:
        amount = Decimal(str(value)).quantize(Decimal("0.01"))
        cents = int(amount * 100)
        if cents <= 0:
            raise ValueError()
        return cents
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("Amount must be a positive number.")

def publish_event(event):
    event_queue.put(event)
    notifications.insert(0, {
        "id": str(uuid.uuid4())[:8],
        "event": event["event"],
        "message": event["message"],
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    del notifications[20:]

@app.route("/")
def home():
    return render_template("index.html")

@app.get("/api/architecture")
def architecture():
    return jsonify({
        "api_gateway": "Flask REST API routes (demo)",
        "services": ["User", "Wallet", "Payment/Transaction", "Fraud Detection", "Notification"],
        "database": "SQLite for local demo; PostgreSQL/MySQL recommended for production",
        "cache": "In-memory balance cache demo; Redis recommended for production",
        "messaging": "In-process queue demo; Kafka/RabbitMQ recommended for production",
        "scaling": ["Run multiple app instances behind a load balancer",
                    "Use database indexes/read replicas/partitioning as traffic grows",
                    "Move notifications and fraud events to a message broker"]
    })

@app.get("/api/wallets/<wallet_id>/balance")
def get_balance(wallet_id):
    with cache_lock:
        if wallet_id in balance_cache:
            return jsonify({"walletId": wallet_id, "balance": money(balance_cache[wallet_id]),
                            "currency": "INR", "source": "cache"})
    with get_db() as db:
        row = db.execute("SELECT balance_cents FROM wallets WHERE wallet_id=?", (wallet_id,)).fetchone()
    if not row:
        return jsonify({"error": "Wallet not found"}), 404
    with cache_lock:
        balance_cache[wallet_id] = row["balance_cents"]
    return jsonify({"walletId": wallet_id, "balance": money(row["balance_cents"]),
                    "currency": "INR", "source": "database"})

@app.post("/api/wallets/<wallet_id>/add-money")
def add_money(wallet_id):
    data = request.get_json(silent=True) or {}
    try:
        cents = parse_amount(data.get("amount"))
    except ValueError as e:
        return jsonify({"status": "FAILED", "message": str(e)}), 400
    with get_db() as db:
        row = db.execute("SELECT balance_cents FROM wallets WHERE wallet_id=?", (wallet_id,)).fetchone()
        if not row:
            return jsonify({"error": "Wallet not found"}), 404
        db.execute("UPDATE wallets SET balance_cents=balance_cents+? WHERE wallet_id=?", (cents, wallet_id))
        new_balance = row["balance_cents"] + cents
        txid = "T" + uuid.uuid4().hex[:10].upper()
        db.execute("""INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?)""",
                   (txid, None, wallet_id, cents, "ADD_MONEY", "SUCCESS", "Money added",
                    datetime.now().isoformat(timespec="seconds")))
    with cache_lock:
        balance_cache[wallet_id] = new_balance
    publish_event({"event": "MoneyAdded", "message": f"₹{money(cents)} added to {wallet_id}", "transactionId": txid})
    return jsonify({"transactionId": txid, "status": "SUCCESS", "balance": money(new_balance),
                    "message": "Money added successfully"}), 201

@app.post("/api/transfers")
def transfer():
    data = request.get_json(silent=True) or {}
    sender, receiver = data.get("senderWalletId"), data.get("receiverWalletId")
    idem = request.headers.get("Idempotency-Key")
    if not sender or not receiver or sender == receiver:
        return jsonify({"status": "FAILED", "message": "Provide different sender and receiver wallets"}), 400
    try:
        cents = parse_amount(data.get("amount"))
    except ValueError as e:
        return jsonify({"status": "FAILED", "message": str(e)}), 400
    # Demo fraud rule: large transfers are flagged for review, not automatically rejected.
    risk_score = 80 if cents >= 1000000 else 10
    txid = "T" + uuid.uuid4().hex[:10].upper()
    now = datetime.now().isoformat(timespec="seconds")
    try:
        with get_db() as db:
            db.execute("BEGIN IMMEDIATE")
            # Idempotency key support for demo: key is stored in message field.
            if idem:
                prior = db.execute("SELECT transaction_id,status,message,amount_cents FROM transactions WHERE message=?",
                                   ("IDEMPOTENCY:" + idem,)).fetchone()
                if prior:
                    return jsonify({"transactionId": prior["transaction_id"], "status": prior["status"],
                                    "message": "Duplicate request; original result returned"}), 200
            s = db.execute("SELECT balance_cents FROM wallets WHERE wallet_id=?", (sender,)).fetchone()
            r = db.execute("SELECT balance_cents FROM wallets WHERE wallet_id=?", (receiver,)).fetchone()
            if not s or not r:
                return jsonify({"status": "FAILED", "message": "Sender or receiver wallet not found"}), 404
            if s["balance_cents"] < cents:
                txid = "T" + uuid.uuid4().hex[:10].upper()
                db.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?)",
                           (txid, sender, receiver, cents, "TRANSFER", "FAILED",
                            "Insufficient balance", now))
                return jsonify({"transactionId": txid, "status": "FAILED",
                                "message": "Insufficient wallet balance"}), 400
            db.execute("UPDATE wallets SET balance_cents=balance_cents-? WHERE wallet_id=?", (cents, sender))
            db.execute("UPDATE wallets SET balance_cents=balance_cents+? WHERE wallet_id=?", (cents, receiver))
            msg = "IDEMPOTENCY:" + idem if idem else "Transfer completed"
            db.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?)",
                       (txid, sender, receiver, cents, "TRANSFER", "SUCCESS", msg, now))
            if risk_score >= 70:
                db.execute("INSERT INTO fraud_alerts VALUES (?,?,?,?,?)",
                           ("F" + uuid.uuid4().hex[:8].upper(), txid, risk_score,
                            "Large transfer requires review", now))
    except sqlite3.Error:
        return jsonify({"status": "FAILED", "message": "Could not safely complete transfer"}), 500
    with cache_lock:
        balance_cache.pop(sender, None)
        balance_cache.pop(receiver, None)
    publish_event({"event": "TransferCompleted", "message": f"₹{money(cents)} transferred from {sender} to {receiver}", "transactionId": txid})
    if risk_score >= 70:
        publish_event({"event": "FraudDetected", "message": f"High-value transfer {txid} flagged for review", "transactionId": txid})
    return jsonify({"transactionId": txid, "status": "SUCCESS", "amount": money(cents),
                    "riskScore": risk_score, "message": "Transfer completed successfully"}), 201

@app.get("/api/transactions")
def transactions():
    wallet_id = request.args.get("walletId")
    with get_db() as db:
        if wallet_id:
            rows = db.execute("""SELECT * FROM transactions
                WHERE sender_wallet=? OR receiver_wallet=? ORDER BY created_at DESC LIMIT 100""",
                              (wallet_id, wallet_id)).fetchall()
        else:
            rows = db.execute("SELECT * FROM transactions ORDER BY created_at DESC LIMIT 100").fetchall()
    return jsonify({"transactions": [{
        "transactionId": r["transaction_id"], "senderWalletId": r["sender_wallet"],
        "receiverWalletId": r["receiver_wallet"], "amount": money(r["amount_cents"]),
        "type": r["transaction_type"], "status": r["status"],
        "message": r["message"], "createdAt": r["created_at"]
    } for r in rows]})

@app.get("/api/transactions/<txid>/download")
def download_transaction(txid):
    with get_db() as db:
        row = db.execute("SELECT * FROM transactions WHERE transaction_id=?", (txid,)).fetchone()
    if not row:
        return jsonify({"error": "Transaction not found"}), 404
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["Transaction ID", "Sender Wallet", "Receiver Wallet", "Amount (INR)", "Type", "Status", "Message", "Created At"])
    writer.writerow([row["transaction_id"], row["sender_wallet"], row["receiver_wallet"], money(row["amount_cents"]),
                     row["transaction_type"], row["status"], row["message"], row["created_at"]])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={txid}_receipt.csv"})

@app.get("/api/fraud-alerts")
def fraud_alerts():
    with get_db() as db:
        rows = db.execute("SELECT * FROM fraud_alerts ORDER BY created_at DESC LIMIT 50").fetchall()
    return jsonify({"alerts": [dict(r) for r in rows]})

@app.get("/api/notifications")
def get_notifications():
    return jsonify({"notifications": notifications[:20], "queuedEvents": event_queue.qsize()})

@app.get("/api/health")
def health():
    return jsonify({"status": "UP", "database": "connected", "cache": "enabled (demo)", "messaging": "enabled (demo)"})

if __name__ == "__main__":
    init_db()
    app.run(debug=True, host="127.0.0.1", port=5000)
