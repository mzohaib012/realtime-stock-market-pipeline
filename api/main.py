from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
import os

load_dotenv()

# Database connection
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine      = create_engine(conn_string)

# Initialize FastAPI
app = FastAPI(
    title       = "🏦 Stock Market API",
    description = "Real-time stock market data API for PSX & NASDAQ",
    version     = "1.0.0"
)

def query_db(sql: str, params: dict = {}):
    """Execute SQL and return results"""
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        rows   = result.fetchall()
        return [dict(row._mapping) for row in rows]

# ─────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────
@app.get("/")
def health_check():
    return {
        "status"  : "healthy 🚀",
        "message" : "Stock Market API is running!",
        "time"    : datetime.now().isoformat()
    }

# ─────────────────────────────────────────
# STOCK ENDPOINTS
# ─────────────────────────────────────────

@app.get("/stocks/latest")
def get_latest_prices():
    """Get latest price for all stocks"""
    rows = query_db("SELECT * FROM latest_stock_prices ORDER BY symbol")
    return {
        "status" : "success",
        "count"  : len(rows),
        "data"   : rows
    }

@app.get("/stocks/{symbol}")
def get_stock(symbol: str):
    """Get data for a specific stock"""
    rows = query_db("""
        SELECT * FROM stock_prices
        WHERE symbol = :symbol
        ORDER BY time DESC
        LIMIT 50
    """, {"symbol": symbol.upper()})

    if not rows:
        raise HTTPException(
            status_code = 404,
            detail      = f"Stock {symbol} not found!"
        )
    return {
        "status" : "success",
        "symbol" : symbol.upper(),
        "count"  : len(rows),
        "data"   : rows
    }

# ─────────────────────────────────────────
# ANALYTICS ENDPOINTS
# ─────────────────────────────────────────

@app.get("/analytics/price-change")
def get_price_change():
    """Get price change analysis for all stocks"""
    rows = query_db("SELECT * FROM price_change_analysis")
    return {
        "status" : "success",
        "count"  : len(rows),
        "data"   : rows
    }

@app.get("/analytics/exchange-comparison")
def get_exchange_comparison():
    """Compare PSX vs NASDAQ performance"""
    rows = query_db("SELECT * FROM exchange_comparison")
    return {
        "status" : "success",
        "data"   : rows
    }

@app.get("/analytics/top-performing")
def get_top_performing():
    """Get top performing stocks"""
    rows = query_db("SELECT * FROM top_performing_stocks")
    return {
        "status" : "success",
        "count"  : len(rows),
        "data"   : rows
    }

@app.get("/analytics/hourly-trend")
def get_hourly_trend():
    """Get hourly price trends"""
    rows = query_db("SELECT * FROM hourly_price_trend LIMIT 100")
    return {
        "status" : "success",
        "count"  : len(rows),
        "data"   : rows
    }

# ─────────────────────────────────────────
# ALERTS ENDPOINTS
# ─────────────────────────────────────────

@app.get("/alerts")
def get_alerts():
    """Get all price alerts"""
    rows = query_db("""
        SELECT * FROM price_alerts
        ORDER BY time DESC
        LIMIT 50
    """)
    return {
        "status" : "success",
        "count"  : len(rows),
        "data"   : rows
    }

@app.get("/alerts/{symbol}")
def get_alerts_by_symbol(symbol: str):
    """Get alerts for specific stock"""
    rows = query_db("""
        SELECT * FROM price_alerts
        WHERE symbol = :symbol
        ORDER BY time DESC
    """, {"symbol": symbol.upper()})
    return {
        "status" : "success",
        "symbol" : symbol.upper(),
        "count"  : len(rows),
        "data"   : rows
    }

# ─────────────────────────────────────────
# SUMMARY ENDPOINT
# ─────────────────────────────────────────

@app.get("/summary")
def get_summary():
    """Get complete market summary"""
    total_stocks   = query_db("SELECT COUNT(DISTINCT symbol) as count FROM stock_prices")[0]["count"]
    total_readings = query_db("SELECT COUNT(*) as count FROM stock_prices")[0]["count"]
    total_alerts   = query_db("SELECT COUNT(*) as count FROM price_alerts")[0]["count"]
    top_stock      = query_db("SELECT symbol, gain_pct FROM top_performing_stocks LIMIT 1")

    return {
        "status"         : "success",
        "total_stocks"   : total_stocks,
        "total_readings" : total_readings,
        "total_alerts"   : total_alerts,
        "top_performer"  : top_stock[0] if top_stock else None,
        "last_updated"   : datetime.now().isoformat()
    }