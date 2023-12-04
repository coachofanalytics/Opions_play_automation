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

# host = os.environ.get('HEROKU_DEV_HOST')
# database_name = os.environ.get('HEROKU_DEV_NAME')
# user_name = os.environ.get('HEROKU_DEV_USER')
# password = os.environ.get('HEROKU_DEV_PASS')
# port='5432'

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
    # Merge liquidity and unusual volume
    csv_file_path_uv = 'unusual_volume.csv'
    csv_file_path_lq = 'liquidity.csv'
    # unusual_df = read_data_from_csv(csv_file_path_uv)[0]
    liquidity_df = read_data_from_csv(csv_file_path_lq)[0]

    # vl_merged_df = pd.merge(unusual_df, liquidity_df[['symbol']], on='symbol', how='inner')
    vl_merged_df =liquidity_df

    # Fetch EBITDA for each symbol in unusual_df
    # vl_merged_df['ebitda'] = vl_merged_df['symbol'].apply(lambda x: fetch_data_util(x).get('ebitda', None))

    # Filter out symbols where EBITDA <= 0
    # positive_ebitda_df = vl_merged_df[vl_merged_df['ebitda'] > -300000]

    # Apply other filters
    # filtered_df = positive_ebitda_df[(positive_ebitda_df['price'] >= 15) & (positive_ebitda_df['volume'] > 1000)]
    positive_ebitda_df =  vl_merged_df
    return positive_ebitda_df

def process_data(df,vl_merged_df):
    new_columns = [x.replace(" ", "_").replace("/", "_").lower() for x in df.columns]
    df.columns = new_columns
    df['expiry'] = pd.to_datetime(df['expiry'], utc=True)  # Convert 'expiry' column to datetime format
    df['comment'] = ' '
    df['on_date'] = ' '
    df['is_active'] = True
    df['is_featured'] = True
    merged_df = pd.merge(df, vl_merged_df[['symbol']], on='symbol', how='inner')
    merged_df['id'] = range(1, len(merged_df) + 1)
    return merged_df