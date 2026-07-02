import json
import logging
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
DB_HOST                 = os.getenv("DB_HOST")
DB_PORT                 = os.getenv("DB_PORT")
DB_NAME                 = os.getenv("DB_NAME")
DB_USER                 = os.getenv("DB_USER")
DB_PASSWORD             = os.getenv("DB_PASSWORD")

TOPIC_RAW_STOCKS   = "raw_stock_data"
TOPIC_PRICE_ALERTS = "price_alerts"

def get_engine():
    conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_string)

def save_stock_price(engine, data):
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO stock_prices 
                (time, symbol, exchange, price, open, high, low, volume, market_cap, currency)
                VALUES 
                (NOW(), :symbol, :exchange, :price, :open, :high, :low, :volume, :market_cap, :currency)
            """), {
                "symbol"     : data.get("symbol"),
                "exchange"   : data.get("exchange"),
                "price"      : data.get("price"),
                "open"       : data.get("open"),
                "high"       : data.get("high"),
                "low"        : data.get("low"),
                "volume"     : data.get("volume"),
                "market_cap" : data.get("market_cap"),
                "currency"   : data.get("currency")
            })
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Error saving: {e}")
        return False

def save_price_alert(engine, data):
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO price_alerts
                (time, symbol, alert_type, prev_price, curr_price, change_pct)
                VALUES
                (NOW(), :symbol, :alert_type, :prev_price, :curr_price, :change_pct)
            """), {
                "symbol"     : data.get("symbol"),
                "alert_type" : data.get("alert_type"),
                "prev_price" : data.get("prev_price"),
                "curr_price" : data.get("curr_price"),
                "change_pct" : data.get("change_pct")
            })
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Error saving alert: {e}")
        return False

def run_consumer():
    logger.info("🚀 Starting Stock Market Consumer!")

    engine = get_engine()
    logger.info("✅ Database connected!")

    consumer = KafkaConsumer(
        TOPIC_RAW_STOCKS,
        TOPIC_PRICE_ALERTS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        key_deserializer=lambda x: x.decode('utf-8') if x else None,
        group_id='stock_consumer_group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        api_version=(2, 6, 0)
    )

    logger.info("✅ Kafka Consumer connected!")
    logger.info("📡 Listening for messages...")

    try:
        for message in consumer:
            topic = message.topic
            data  = message.value

            if topic == TOPIC_RAW_STOCKS:
                success = save_stock_price(engine, data)
                if success:
                    logger.info(
                        f"💾 Saved: {data.get('symbol'):12} | "
                        f"Price: {data.get('price'):10.2f} | "
                        f"Exchange: {data.get('exchange')}"
                    )

            elif topic == TOPIC_PRICE_ALERTS:
                success = save_price_alert(engine, data)
                if success:
                    logger.warning(
                        f"🔔 Alert: {data.get('symbol')} | "
                        f"{data.get('alert_type')} | "
                        f"{data.get('change_pct')}%"
                    )

    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped!")
        consumer.close()

if __name__ == "__main__":
    run_consumer()