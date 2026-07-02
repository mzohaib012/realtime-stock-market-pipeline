import yfinance as yf
import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
FETCH_INTERVAL          = int(os.getenv("FETCH_INTERVAL", 300))

# Stocks to track
PSX_STOCKS = os.getenv("PSX_STOCKS").split(",")
US_STOCKS  = os.getenv("US_STOCKS").split(",")
ALL_STOCKS = PSX_STOCKS + US_STOCKS

# Kafka Topics
TOPIC_RAW_STOCKS    = "raw_stock_data"
TOPIC_PRICE_ALERTS  = "price_alerts"

def create_producer():
    """Create Kafka Producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8'),
            retries=3,
            acks='all'
        )
        logger.info("✅ Kafka Producer connected successfully!")
        return producer
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kafka: {e}")
        raise

def fetch_stock_data(symbol: str) -> dict:
    """Fetch live stock data from Yahoo Finance"""
    try:
        ticker = yf.Ticker(symbol)
        info   = ticker.fast_info

        data = {
            "symbol"        : symbol,
            "exchange"      : "PSX" if ".KA" in symbol else "NASDAQ",
            "price"         : round(float(info.last_price), 2) if info.last_price else None,
            "open"          : round(float(info.open), 2) if info.open else None,
            "high"          : round(float(info.day_high), 2) if info.day_high else None,
            "low"           : round(float(info.day_low), 2) if info.day_low else None,
            "volume"        : int(info.three_month_average_volume) if info.three_month_average_volume else None,
            "market_cap"    : float(info.market_cap) if info.market_cap else None,
            "currency"      : info.currency if info.currency else None,
            "timestamp"     : datetime.utcnow().isoformat(),
            "fetch_time"    : datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }
        return data

    except Exception as e:
        logger.error(f"❌ Error fetching {symbol}: {e}")
        return None

def check_price_alert(data: dict, prev_price: float) -> dict:
    """Check if price changed by more than 5%"""
    if not prev_price or not data.get("price"):
        return None

    change_pct = ((data["price"] - prev_price) / prev_price) * 100

    if abs(change_pct) >= 5:
        return {
            "symbol"      : data["symbol"],
            "alert_type"  : "PRICE_SURGE" if change_pct > 0 else "PRICE_DROP",
            "prev_price"  : prev_price,
            "curr_price"  : data["price"],
            "change_pct"  : round(change_pct, 2),
            "timestamp"   : datetime.utcnow().isoformat()
        }
    return None

def run_producer():
    """Main producer loop"""
    logger.info("🚀 Starting Stock Market Producer!")
    logger.info(f"📊 Tracking {len(ALL_STOCKS)} stocks: {ALL_STOCKS}")
    logger.info(f"⏰ Fetching every {FETCH_INTERVAL} seconds")

    producer    = create_producer()
    prev_prices = {}

    while True:
        try:
            logger.info(f"\n{'='*50}")
            logger.info(f"📡 Fetching stock data at {datetime.utcnow()}")

            for symbol in ALL_STOCKS:
                # Fetch stock data
                data = fetch_stock_data(symbol)

                if data and data.get("price"):
                    # Send to Kafka raw topic
                    producer.send(
                        TOPIC_RAW_STOCKS,
                        key=symbol,
                        value=data
                    )
                    logger.info(
                        f"✅ {symbol:12} | "
                        f"Price: {data['price']:10.2f} | "
                        f"Exchange: {data['exchange']}"
                    )

                    # Check for price alerts
                    alert = check_price_alert(
                        data,
                        prev_prices.get(symbol)
                    )

                    if alert:
                        producer.send(
                            TOPIC_PRICE_ALERTS,
                            key=symbol,
                            value=alert
                        )
                        logger.warning(
                            f"🔔 ALERT: {alert['symbol']} "
                            f"{alert['alert_type']} "
                            f"{alert['change_pct']}%"
                        )

                    # Update previous price
                    prev_prices[symbol] = data["price"]

                else:
                    logger.warning(f"⚠️ No data for {symbol}")

                # Small delay between stocks
                time.sleep(1)

            # Flush all messages
            producer.flush()
            logger.info(f"✅ All stocks processed!")
            logger.info(f"⏳ Waiting {FETCH_INTERVAL} seconds...")
            time.sleep(FETCH_INTERVAL)

        except KeyboardInterrupt:
            logger.info("\n🛑 Producer stopped!")
            producer.close()
            break

        except Exception as e:
            logger.error(f"❌ Error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    run_producer()