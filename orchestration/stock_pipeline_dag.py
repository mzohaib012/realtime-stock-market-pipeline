from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

# ─── Default Arguments ───
default_args = {
    'owner'           : 'stock_market_pipeline',
    'depends_on_past' : False,
    'start_date'      : datetime(2026, 4, 9),
    'retries'         : 3,
    'retry_delay'     : timedelta(minutes=5),
    'email_on_failure': False,
}

# ─── DAG Definition ───
dag = DAG(
    'stock_market_pipeline',
    default_args     = default_args,
    description      = 'Real-time stock market data pipeline',
    schedule_interval= '*/5 * * * *',  # Every 5 minutes
    catchup          = False,
    tags             = ['stock', 'market', 'pipeline']
)

# ─── Task 1: Health Check ───
def check_kafka_health():
    """Check if Kafka is running"""
    from kafka import KafkaConsumer
    try:
        consumer = KafkaConsumer(
            bootstrap_servers='192.168.100.218:9092',
            api_version=(2, 6, 0)
        )
        consumer.close()
        print("✅ Kafka is healthy!")
        return True
    except Exception as e:
        raise Exception(f"❌ Kafka health check failed: {e}")

# ─── Task 2: Fetch Stock Data ───
def fetch_and_produce():
    """Fetch stock data and send to Kafka"""
    import yfinance as yf
    import json
    from kafka import KafkaProducer
    from datetime import datetime

    producer = KafkaProducer(
        bootstrap_servers='192.168.100.218:9092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8'),
        api_version=(2, 6, 0)
    )

    stocks = [
        'OGDC.KA', 'HBL.KA', 'LUCK.KA', 'PSO.KA', 'ENGRO.KA',
        'AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN'
    ]

    for symbol in stocks:
        try:
            ticker = yf.Ticker(symbol)
            info   = ticker.fast_info

            data = {
                "symbol"    : symbol,
                "exchange"  : "PSX" if ".KA" in symbol else "NASDAQ",
                "price"     : round(float(info.last_price), 2) if info.last_price else None,
                "open"      : round(float(info.open), 2) if info.open else None,
                "high"      : round(float(info.day_high), 2) if info.day_high else None,
                "low"       : round(float(info.day_low), 2) if info.day_low else None,
                "volume"    : int(info.three_month_average_volume) if info.three_month_average_volume else None,
                "currency"  : info.currency if info.currency else None,
                "timestamp" : datetime.now().isoformat()
            }

            producer.send("raw_stock_data", key=symbol, value=data)
            print(f"✅ Sent: {symbol} → {data['price']}")

        except Exception as e:
            print(f"⚠️ Error fetching {symbol}: {e}")

    producer.flush()
    producer.close()
    print("✅ All stocks sent to Kafka!")

# ─── Task 3: Validate Data ───
def validate_data():
    """Check data was saved to database"""
    from sqlalchemy import create_engine, text

    engine = create_engine(
        "postgresql://postgres:admin123@localhost:5432/stock_market_db"
    )

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) as count 
            FROM stock_prices 
            WHERE time > NOW() - INTERVAL '10 minutes'
        """))
        count = result.fetchone()[0]
        print(f"✅ {count} records saved in last 10 minutes!")

        if count == 0:
            raise Exception("❌ No data saved in last 10 minutes!")

# ─── Define Tasks ───
task_health_check = PythonOperator(
    task_id         = 'check_kafka_health',
    python_callable = check_kafka_health,
    dag             = dag
)

task_fetch_stocks = PythonOperator(
    task_id         = 'fetch_and_produce',
    python_callable = fetch_and_produce,
    dag             = dag
)

task_validate = PythonOperator(
    task_id         = 'validate_data',
    python_callable = validate_data,
    dag             = dag
)

# ─── Task Dependencies ───
# health_check → fetch_stocks → validate
task_health_check >> task_fetch_stocks >> task_validate