from projeto import *

if __name__ == '__main__':

    print('iniciando')
    result_exchange = get_exchange(valid_exchanges=VALID_EXCHANGES)
    dataframe_exchange = process_exchange_etl(df=result_exchange)

    results_market = get_markets(exchange_ids=VALID_EXCHANGES, base_ids=VALID_COINS)
    dataframe_market = process_market_etl(df=results_market)

    results_coin = get_coins(valid_coins=VALID_COINS)
    dataframe_coins = process_coin_etl(df=results_coin)

    save_on_mysql(dataframe=dataframe_exchange, tabela='tb_api_exchange', if_exists='replace')
    save_on_mysql(dataframe=dataframe_coins, tabela='tb_api_coins', if_exists='replace')
    save_on_mysql(dataframe=dataframe_market, tabela='tb_api_markets', if_exists='append')
