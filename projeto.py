import requests
import pandas as pd
import pymysql
import os 
from urllib.parse import urljoin
from sqlalchemy import create_engine
from constants import *

def columns_snake(df):
    df.columns = df.columns.str.replace(r'(?<!^)(?=[A-Z])', '_', regex=True).str.lower()
    return df

def get_request_config(
        limit: int = 2000,
        offset:int = 1
        ):

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept-Encoding": "gzip"
    }

    params = {
        "limit": limit,
        "offset": offset
    }

    return headers, params

def get_exchange(valid_exchanges=None):
    """Tabela dimensão de corretoras"""
    i = 0
    empty = True 
    all_data = []  

    while empty:
        headers, payload = get_request_config(2000, i)
        url = urljoin(DEFAULT_URL, ENDPOINTS["exchanges"])
        
        full_response = requests.get(url, headers=headers, data=payload)
        response = full_response.json()
        data = response.get("data", [])

        if not data:
            break

        all_data.extend(data)

        if len(data) < 2000:  
            empty = False

        i += 1 

    if not valid_exchanges:
        return all_data
    
    filtered_data = [item for item in all_data if item["exchangeId"] in valid_exchanges]

    return filtered_data

def process_exchange_etl(df: pd.DataFrame):
    df = pd.DataFrame(df)
    df = columns_snake(df=df)
    df["updated"] = pd.to_datetime(df["updated"], unit="ms")
    df["updated"] = df["updated"] - pd.to_timedelta(3, unit="h")
    numeric_cols = ["percent_total_volume", "volume_usd", "trading_pairs"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates()

    return df

def get_markets(
        exchange_ids: list[str] = [""], 
        base_ids: list[str] = [""]
    ):

    i = 0
    results = []  

    while True:
        # print(f"Iteração: {i}")
        headers, payload = get_request_config(2000, i)

        all_exchanges_empty = True  

        for exchange_id in exchange_ids:
            # print(f"Verificando exchange: {exchange_id}")
            found_empty_data_per_exchange = False 

            for base_id in base_ids:
                url = urljoin(DEFAULT_URL, ENDPOINTS["markets"]
                            .replace("{exchange_id}", exchange_id)
                            .replace("{base_id}", base_id)
                        )

                full_response = requests.get(url, headers=headers, params=payload)
                response = full_response.json()
                data = response.get('data', [])
                # print(data)
                #print(f"Exchange: {exchange_id} | Base: {base_id} | Tamanho dos dados: {len(data)}")

                results.extend(data)

                if len(data) > 0:
                    all_exchanges_empty = False 
                else:
                    found_empty_data_per_exchange = True  
            
            if found_empty_data_per_exchange:
                #print(f"Exchange {exchange_id} não retornou dados.")
                continue  

        if all_exchanges_empty:
            # print("len(data) = 0 em tudo; Encerrando o loop.")
            break  
        
        i += 1 

    return results

def process_market_etl(df: pd.DataFrame):
    df = pd.DataFrame(df)
    df = columns_snake(df=df)
    df["updated"] = pd.to_datetime(df["updated"], unit="ms")
    df["updated"] = df["updated"] - pd.to_timedelta(3, unit="h")
    numeric_cols = ["price_quote", "price_usd", "volume_usd24_hr", "percent_exchange_volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.drop_duplicates()

    return df

def get_coins(valid_coins=None):
    """Tabela dimensão coins"""
    i = 0
    empty = True  
    all_data = []  

    while empty:
        headers, payload = get_request_config(2000, i)
        url = urljoin(DEFAULT_URL, ENDPOINTS["coins"])

        full_response = requests.get(url, headers=headers, data=payload)
        response = full_response.json()
        data = response.get("data", [])
        if not data:
            break 

        all_data.extend(data)

        if len(data) < 2000:  
            empty = False 

        i += 1 

    if not valid_coins:
        return all_data 

    filtered_data = [item for item in all_data if item["id"] in valid_coins]

    return filtered_data

def process_coin_etl(df: pd.DataFrame):
    df = pd.DataFrame(df)
    df = columns_snake(df)
    numeric_cols = ["supply", "max_supply", "market_cap_usd", "volume_usd24_hr", "price_usd", "change_percent24_hr", "vwap24_hr"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.drop_duplicates()

    return df

def save_on_mysql(dataframe: pd.DataFrame, tabela: str, if_exists: str = "replace"):
    """
    Salva Dataframe no mysql.

    Parâmetros:
    - dataframe: Espera-se um dataframe;
    - tabela (str): Nome da tabela no mysql;
    - if_exists (str): Comportamento caso a tabela já exista. Opções:
        - "fail": Levanta um erro se a tabela já existir.
        - "replace": Substitui a tabela existente.
        - "append": Adiciona os novos dados à tabela existente.

    Retorno:
    - None
    """
    
    if not isinstance(dataframe, pd.DataFrame):
        raise ValueError("O objeto passado não é um DataFrame válido.")

    try:
        print(f"📡 Conectando ao banco de dados MySQL...")
        engine = create_engine(f"mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}")
        dataframe.to_sql(tabela, con=engine, if_exists=if_exists, index=False)

        print(f"Dados salvos: `{tabela}`!")

    except Exception as e:
        print(f"Erro ao salvar: {e}")

    finally:
        engine.dispose()
        print("Conexão encerrada.")
