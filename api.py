from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import psycopg2
from collections import Counter

app = FastAPI(title="Enterprise DQ Monitoring API")

DB_PARAMS = {
    "dbname": "vkr_db", "user": "vkr_user",
    "password": "vkr_password", "host": "localhost", "port": "5432"
}

@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/alerts")
def get_recent_alerts():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    try:
        # Достаем детали транзакции
        cur.execute("""
            SELECT transaction_id, error_reason, created_at, amount, currency, source_system 
            FROM quarantine_transactions ORDER BY created_at DESC LIMIT 50;
        """)
        rows = cur.fetchall()
        alerts = [{
            "tx_id": r[0], "reason": r[1], "timestamp": str(r[2]),
            "amount": r[3], "currency": r[4], "system": r[5]
        } for r in rows]

        cur.execute("SELECT COUNT(*) FROM quarantine_transactions;")
        total_errors = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM core_transactions;")
        total_valid = cur.fetchone()[0]

        return {"status": "success", "total_alerts": total_errors, "total_valid": total_valid, "alerts": alerts}
    except Exception as e:
        return {"status": "error", "message": "Сделайте запуск dbt run"}
    finally:
        conn.close()

@app.get("/api/chart-data")
def get_chart_data():
    """Эндпоинт для графиков Chart.js"""
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    try:
        cur.execute("SELECT error_reason FROM quarantine_transactions LIMIT 500;")
        rows = cur.fetchall()
        reasons = [r[0] for r in rows]
        counter = Counter(reasons)
        return {"labels": list(counter.keys()), "values": list(counter.values())}
    except Exception:
        return {"labels": [], "values": []}
    finally:
        conn.close()

@app.get("/api/dqi")
def get_dqi_metrics():
    """Эндпоинт для получения Data Quality Index по стандарту DAMA-DMBOK"""
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    try:
        cur.execute("SELECT completeness, validity, timeliness, dqi_score FROM gold_dq_metrics;")
        row = cur.fetchone()
        if row:
            return {
                "status": "success",
                "completeness": float(row[0]),
                "validity": float(row[1]),
                "timeliness": float(row[2]),
                "dqi": float(row[3])
            }
        return {"status": "error", "message": "Нет данных"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()