# market_data_producer.py

import time
import json
import ccxt
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TICKER_TOPIC = 'btc_usd_ticker'
KAFKA_ORDER_BOOK_TOPIC = 'btc_order_book'
KAFKA_TRADES_TOPIC = 'btc_usd_trades'
SYMBOL = 'BTC/USDT'
FETCH_INTERVAL = 10  # seconds between fetches


def create_producer():
    """
    Create a Kafka producer with JSON serialization.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5  # Retry up to 5 times on failure
    )


def fetch_ticker(exchange):
    """
    Fetch ticker data using CCXT.
    Returns a dictionary with relevant ticker fields.
    """
    ticker = exchange.fetch_ticker(SYMBOL)
    parsed_ticker = {
        'symbol': ticker['symbol'],
        'timestamp': ticker['timestamp'],  # in milliseconds
        'datetime': ticker['datetime'],
        'high': float(ticker['high']),
        'low': float(ticker['low']),
        'bid': float(ticker['bid']),
        'ask': float(ticker['ask']),
        'vwap': float(ticker['vwap']) if ticker['vwap'] else None,
        'open': float(ticker['open']),
        'close': float(ticker['close']),
        'last': float(ticker['last']),
        'previousClose': float(ticker['previousClose']),
        'change': float(ticker['change']),
        'percentage': float(ticker['percentage']),
        'average': float(ticker['average']),
        'baseVolume': float(ticker['baseVolume']),
        'quoteVolume': float(ticker['quoteVolume']),
        'info': ticker['info']  # Raw exchange data
    }
    return parsed_ticker


def fetch_order_book(exchange):
    """
    Fetch order book data using CCXT.
    Returns a dictionary with relevant order book fields.
    """
    order_book = exchange.fetch_order_book(SYMBOL)
    parsed_order_book = {
        'symbol': order_book['symbol'],
        'timestamp': order_book['timestamp'],  # in milliseconds
        'datetime': order_book['datetime'],
        'bids': [[float(price), float(amount)] for price, amount in order_book['bids']],
        'asks': [[float(price), float(amount)] for price, amount in order_book['asks']],
        'info': order_book['info']  # Raw exchange data
    }
    return parsed_order_book


def fetch_trades(exchange):
    """
    Fetch recent trades using CCXT.
    Returns a list of dictionaries with relevant trade fields.
    """
    trades = exchange.fetch_trades(SYMBOL)
    parsed_trades = []
    for trade in trades:
        parsed_trades.append({
            'symbol': trade['symbol'],
            'timestamp': trade['timestamp'],  # in milliseconds
            'datetime': trade['datetime'],
            'side': trade['side'],
            'price': float(trade['price']),
            'amount': float(trade['amount']),
            'cost': float(trade['cost']) if trade['cost'] else None,
            'info': trade['info']  # Raw exchange data
        })
    return parsed_trades


def main():
    producer = create_producer()
    exchange = ccxt.binance({'enableRateLimit': True})

    print(f"[Producer] Starting, publishing data for {SYMBOL} to Kafka topics...")

    while True:
        try:
            # Fetch and publish ticker data
            ticker = fetch_ticker(exchange)
            producer.send(KAFKA_TICKER_TOPIC, value=ticker)

            # Fetch and publish order book data
            order_book = fetch_order_book(exchange)
            producer.send(KAFKA_ORDER_BOOK_TOPIC, value=order_book)

            # Fetch and publish recent trades
            trades = fetch_trades(exchange)
            for trade in trades:
                producer.send(KAFKA_TRADES_TOPIC, value=trade)

            producer.flush()
            print(f"[Producer] Sent ticker, order book, and {len(trades)} trades to respective topics.")

            time.sleep(FETCH_INTERVAL)

        except KafkaError as ke:
            print(f"[Producer] Kafka error: {ke}")
            time.sleep(5)  # Wait before retrying
        except ccxt.NetworkError as ne:
            print(f"[Producer] Network error: {ne}")
            time.sleep(5)
        except ccxt.ExchangeError as ee:
            print(f"[Producer] Exchange error: {ee}")
            time.sleep(5)
        except Exception as e:
            print(f"[Producer] Unexpected error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
