import os 
from dotenv import find_dotenv, load_dotenv

DEFAULT_URL = 'https://api.coincap.io/v2/'

ENDPOINTS = {
    'candles': 'candles?exchange={exchange}&interval={interval}&baseId={baseId}&quoteId={quote_id}',
    'coins': 'assets',
    'markets': 'markets?exchangeId={exchange_id}&baseId={base_id}',
    'exchanges': 'exchanges'

}
_ = load_dotenv(find_dotenv())

API_KEY = os.getenv('API_KEY')

DB_CONNECTION= os.getenv('DB_CONNECTION')
DB_HOST = os.getenv('DB_HOST')
DB_PORT=int(os.getenv('DB_PORT'))
DB_DATABASE=os.getenv('DB_DATABASE')
DB_USERNAME=os.getenv('DB_USERNAME')
DB_PASSWORD=os.getenv('DB_PASSWORD')


VALID_COINS = ['bitcoin', 'ethereum', 'xrp', 'solana', 'cardano', 'litecoin', 'dogecoin', 'chainlink', 'ava', 'avalanche', 'polkadot']
# VALID_COINS = ['bitcoin']
VALID_EXCHANGES = ['binance', 'gdax', 'crypto', 'kucoin', 'gate', 'huobi', 'bitmax']