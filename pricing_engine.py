# pricing_engine.py

from kafka import KafkaConsumer, KafkaProducer
import json
import pandas as pd
import numpy as np
from scipy.stats import norm
import time
import uuid
from sqlalchemy import create_engine
from datetime import datetime

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'btc_usd_ticker',
    'btc_order_book',
    'btc_usd_trades',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='pricing_engine_group',
    value_serializer=lambda m: json.loads(m.decode('utf-8'))
)

# Initialize Kafka Producer for option prices
price_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Initialize Database Connection (Example with TimescaleDB)
engine = create_engine('postgresql://username:password@localhost:5432/option_pricing')

def black_scholes(S, K, T, r, sigma, option_type='call'):
    """Black-Scholes option pricing model."""
    d1 = (np.log(S / K) + (r + 0.5 * sigma **2 ) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    if option_type == 'call':
        price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    elif option_type == 'put':
        price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
    return price

def process_data(message):
    """Process incoming Kafka messages."""
    data = message.value
    topic = message.topic

    # Example: Only process ticker data for option pricing
    if topic == 'btc_usd_ticker':
        S = data['last']  # Current Bitcoin price
        K = 50000  # Example strike price
        T = 30 / 365  # 30 days to expiration
        r = 0.01  # Example risk-free rate
        sigma = 0.5  # Example volatility (50%)

        # Compute option prices
        call_price = black_scholes(S, K, T, r, sigma, 'call')
        put_price = black_scholes(S, K, T, r, sigma, 'put')

        option_data = {
            'option_id': str(uuid.uuid4()),
            'timestamp': datetime.utcnow().isoformat(),
            'underlying_price': S,
            'strike_price': K,
            'volatility': sigma,
            'interest_rate': r,
            'option_price_call': call_price,
            'option_price_put': put_price,
            'pricing_method': 'Black-Scholes'
        }

        # Publish to Kafka
        price_producer.send('btc_options_quotes', option_data)
        price_producer.flush()

        # Store in Database
        with engine.connect() as connection:
            connection.execute("""
                INSERT INTO btc_option_prices (
                    option_id, timestamp, underlying_price, strike_price, 
                    volatility, interest_rate, option_price, pricing_method
                ) VALUES (
                    %(option_id)s, %(timestamp)s, %(underlying_price)s, %(strike_price)s, 
                    %(volatility)s, %(interest_rate)s, %(option_price_call)s, %(pricing_method)s
                )
            """, option_data)

def monte_carlo_simulation(S, K, T, r, sigma, option_type='call', num_simulations=10000):
    """Monte Carlo simulation for option pricing."""
    dt = T
    rand = np.random.normal(0, 1, num_simulations)
    S_T = S * np.exp((r - 0.5 * sigma **2 ) * T + sigma * np.sqrt(T) * rand)
    if option_type == 'call':
        payoffs = np.maximum(S_T - K, 0)
    elif option_type == 'put':
        payoffs = np.maximum(K - S_T, 0)
    price = np.exp(-r * T) * np.mean(payoffs)
    return price

def process_data_2(message):
    """Process incoming Kafka messages."""
    data = message.value
    topic = message.topic

    # Example: Only process ticker data for option pricing
    if topic == 'btc_usd_ticker':
        S = data['last']  # Current Bitcoin price
        K = 50000  # Example strike price
        T = 30 / 365  # 30 days to expiration
        r = 0.01  # Example risk-free rate
        sigma = 0.5  # Example volatility (50%)

        # Compute option prices using Monte Carlo
        call_price_mc = monte_carlo_simulation(S, K, T, r, sigma, 'call')
        put_price_mc = monte_carlo_simulation(S, K, T, r, 'put')

        # Add Monte Carlo prices to option_data
        option_data.update({
            'option_price_call_mc': call_price_mc,
            'option_price_put_mc': put_price_mc,
            'pricing_method_mc': 'Monte Carlo'
        })


        # publish and store data
        # Publish to Kafka
        price_producer.send('btc_options_quotes', option_data)
        price_producer.flush()

        # Store in Database
        with engine.connect() as connection:
            connection.execute("""
                INSERT INTO btc_option_prices (
                    option_id, timestamp, underlying_price, strike_price, 
                    volatility, interest_rate, option_price, pricing_method
                ) VALUES (
                    %(option_id)s, %(timestamp)s, %(underlying_price)s, %(strike_price)s, 
                    %(volatility)s, %(interest_rate)s, %(option_price_call)s, %(pricing_method)s
                )
            """, option_data)


if __name__ == "__main__":
    for message in consumer:
        process_data(message)
        process_data_2(message)

