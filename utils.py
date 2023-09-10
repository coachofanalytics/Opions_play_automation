import json
import os
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import yfinance as yf

try:
    SOME_SECRET = os.environ["SOME_SECRET"]
except KeyError:
    SOME_SECRET = "Token not available!"
    #logger.info("Token not available!")
    #raise

user_name= os.environ['USER']
password= os.environ['PASSWORD']
host= os.environ['HOST']
port='5432'
database_name = os.environ['DATABASE']

USERNAME = os.environ['USERNAME']
KEY = os.environ['KEY']


DB_CONFIG = {
    'dbname': database_name,
    'user': user_name,
    'password': password,
    'host': host,
    'port': port
}



def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def load_data_from_db(ticker_symbol):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM investing_ticker_data WHERE symbol = %s AND fetched_date >= %s", 
                       (ticker_symbol, datetime.now() - timedelta(days=30)))
        data = cursor.fetchone()

        cursor.close()
        conn.close()
        return data
    except Exception as e:
        print(f"Error fetching data from DB for {ticker_symbol}: {e}")
        return None

def save_data_to_db(ticker_symbol, data):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        insert_sql = """
            INSERT INTO investing_ticker_data
            (symbol, overallrisk, sharesshort, enterpriseToEbitda, ebitda, 
             quickratio, currentratio, revenuegrowth, fetched_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        cursor.execute(
            insert_sql, 
            (
                ticker_symbol, 
                data.get('overallRisk', None), 
                data.get('sharesShort', None), 
                data.get('enterpriseToEbitda', None),
                data.get('ebitda', None),
                data.get('quickRatio', None),
                data.get('currentRatio', None),
                data.get('revenueGrowth', None),
                datetime.now().date()  # Current date
            )
        )

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error saving data to DB for {ticker_symbol}: {e}")

def fetch_data_util(ticker_symbol):
    db_data = load_data_from_db(ticker_symbol)
    
    if db_data:
        columns = ["symbol", "overallRisk", "sharesShort", "enterpriseToEbitda", "ebitda", "quickRatio", "currentRatio", "revenueGrowth", "fetched_date"]
        data = dict(zip(columns, db_data))
    else:
        try:
            ticker_data = yf.Ticker(ticker_symbol)
            full_data = ticker_data.info
            valuation_keys = ["overallRisk", "sharesShort", "enterpriseToEbitda", "ebitda", "quickRatio", "currentRatio", "revenueGrowth"]
            data = {key: full_data.get(key, None) for key in valuation_keys}
            
            save_data_to_db(ticker_symbol, data)
        except Exception as e:
            print(f"Error fetching data from yfinance for {ticker_symbol}: {e}")
            return {}
    return data

def unusual_volume():
    # path='C:/Users/CHRIS/web/opa/app/data/unusual_volume.csv'
    # udf = pd.read_csv(path, encoding='utf-8') if os.path.exists(path) else print("File does not exist")
    udf= pd.read_csv('unusual_volume.csv')
    new_columns = [x.replace(" ", "_").replace("/", "_").lower() for x in udf.columns]
    udf.columns = new_columns
    udf['price'] = udf['price'].astype(float)
    udf['volume'] = udf['volume'].astype(float)
    filtered_udf = udf[(udf['volume'] > 1000) & (udf['price'] >= 15)  & (udf['price'] <= 150)]
    return filtered_udf

def liquidity():
    #getting liquidity
    ldf= pd.read_csv('liquidity.csv')
    new_columns = [x.replace(" ", "_").replace("/", "_").lower() for x in ldf.columns]
    ldf.columns = new_columns
    ldf['liquidity'] = ldf['liquidity'].astype(float)
    # converstion and cleanup
    ldf['liquidity'] = ldf['liquidity'].astype(float)
    # filtered_df = df[(df['rank'] > 15) & (df['rank'] <= 75) & (df['price'] >= 15)]
    filtered_ldf = ldf[(ldf['liquidity'] == 1)]
    return filtered_ldf

def oversold_overbought():
    #getting liquidity
    osb_df= pd.read_csv('oversold_overbought_09102023.csv')
    new_columns = [x.replace(" ", "_").replace("/", "_").lower() for x in osb_df.columns]
    osb_df.columns = new_columns
    return osb_df


def merged_data():
    # Merge liquidity and unusual volume
    unusual_df = unusual_volume()
    liquidity_df = liquidity()
    vl_merged_df = pd.merge(unusual_df,liquidity_df [['symbol']], on='symbol', how='inner')
    vl_merged_df_columns = [x.replace(" ", "_").replace("/", "_").lower() for x in vl_merged_df.columns]

    # Fetch EBITDA for each symbol in unusual_df
    vl_merged_df['ebitda'] = vl_merged_df['symbol'].apply(lambda x: fetch_data_util(x).get('ebitda', None))

    # Filter out symbols where EBITDA <= 0
    positive_ebitda_df = vl_merged_df[vl_merged_df['ebitda'] > 0]

    # Apply other filters
    filtered_df = positive_ebitda_df[(positive_ebitda_df['price'] >= 15) & (positive_ebitda_df['volume'] > 1000)]
    return filtered_df
