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
    
def load_oversolddata_from_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM investing_overboughtsold")
        data = cursor.fetchone()

        cursor.close()
        conn.close()
        return data
    except Exception as e:
        print(f"Error fetching data failed: {e}")
        return None

def read_data_from_csv(csv_file_path):
    try:
        csv_df= pd.read_csv(csv_file_path)
        csv_df_dict = csv_df.to_dict(orient='records')
    except Exception as e:
        print(f"Error reading data from CSV file: {e}")
        return []
    return csv_df,csv_df_dict


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

def fetch_oversold_util():
    db_data = load_oversolddata_from_db()
    
    if db_data:
        columns = ["symbol"]
        data = dict(zip(columns, db_data))
    else:
        try:
            csv_file_path_osb='oversold_overbought_09102023.csv'
            data= read_data_from_csv(csv_file_path_osb)[0]
            # save_oversold_data_to_db(data)

        except Exception as e:
            print(f"Error fetching data from yfinance : {e}")
            return {}
    return data

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


def merged_data():
    db_data = fetch_oversold_util()
    # Merge liquidity and unusual volume
    csv_file_path_uv='unusual_volume.csv'
    csv_file_path_lq='liquidity.csv'
    csv_file_path_osb='oversold_overbought_09102023.csv'
    unusual_df = read_data_from_csv(csv_file_path_uv)[0]
    liquidity_df = read_data_from_csv(csv_file_path_lq)[0]
    osb_df = db_data #read_data_from_csv(csv_file_path_osb)[0]

    vl_merged_df = pd.merge(unusual_df,liquidity_df [['symbol']], on='symbol', how='inner')

    merged_df = pd.merge(vl_merged_df, osb_df[['symbol']], on='symbol', how='inner')

    # Fetch EBITDA for each symbol in unusual_df
    merged_df['ebitda'] = merged_df['symbol'].apply(lambda x: fetch_data_util(x).get('ebitda', None))

    # Filter out symbols where EBITDA <= 0
    positive_ebitda_df = merged_df[merged_df['ebitda'] > -300000]

    # Apply other filters
    # filtered_df = positive_ebitda_df[(positive_ebitda_df['price'] >= 15) & (positive_ebitda_df['volume'] > 1000)]
    # filtered_df = positive_ebitda_df

    return positive_ebitda_df
