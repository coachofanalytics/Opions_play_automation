import logging
import logging.handlers
import os
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import subprocess
import pandas as pd
import yfinance as yf
import datetime
import psycopg2
# user_name= os.environ.get('HEROKU_DEV_USER')
# password= os.environ.get('HEROKU_DEV_PASS')
# host= os.environ.get('HEROKU_DEV_HOST')
# port='5432'
# database_name =  os.environ.get('HEROKU_DEV_NAME')

host = 'localhost'
dbname = "CODA_DEV" 
user = "postgres" 
password ="MANAGER2030"
port='5432'


# def fetch_data_util(ticker_symbol):
#     ticker_data = yf.Ticker(ticker_symbol)
#     # Fetch key statistics and then extract only valuation measures
#     full_data = ticker_data.info
#     ''' ============== All finacial terms =============   '''
#     valuation_keys = ["overallRisk", "sharesShort", "enterpriseToEbitda", "ebitda", "quickRatio", "currentRatio", "revenueGrowth"]
#     data = {key: full_data.get(key, None) for key in valuation_keys}
#     return data

# def unusual_volume():
#     path='C:/Users/CHRIS/web/opa/app/data/unusual_volume.csv'
#     unusual_df = pd.read_csv(path, encoding='utf-8') if os.path.exists(path) else print("File does not exist")
#     new_columns = [x.replace(" ", "_").replace("/", "_").lower() for x in unusual_df.columns]
#     unusual_df.columns = new_columns
#     return unusual_df


def fetch_data_util(ticker_symbol):
    ticker_data = yf.Ticker(ticker_symbol)
    # Fetch key statistics and then extract only valuation measures
    full_data = ticker_data.info
    ''' ============== All financial terms =============   '''
    valuation_keys = ["overallRisk", "sharesShort", "enterpriseToEbitda", "ebitda", "quickRatio", "currentRatio", "revenueGrowth"]
    data = {key: full_data.get(key, None) for key in valuation_keys}
    return data

def unusual_volume():
    path='C:/Users/CHRIS/web/opa/app/data/unusual_volume.csv'
    udf = pd.read_csv(path, encoding='utf-8') if os.path.exists(path) else print("File does not exist")
    new_columns = [x.replace(" ", "_").replace("/", "_").lower() for x in udf.columns]
    udf.columns = new_columns
    udf['price'] = udf['price'].astype(float)
    udf['volume'] = udf['volume'].astype(float)

    # Fetch EBITDA for each symbol in unusual_df
    udf['ebitda'] = udf['symbol'].apply(lambda x: fetch_data_util(x).get('ebitda', None))
    
    # Filter out symbols where EBITDA <= 0
    positive_ebitda_df = udf[udf['ebitda'] > 0]

    filtered_df = positive_ebitda_df[(positive_ebitda_df['price'] >= 15) & (positive_ebitda_df['volume'] > 1000)]
    # Print total records from both files
    # print(f"Total records in unusualvolume.csv: {len(udf)}")
    # print(f"Total records in positive_ebitda_df: {len(positive_ebitda_df)}")
    # print(f"Total records in filtered_df with +ve ebitda: {len(filtered_df)}")

    return filtered_df

# def merged_data():
#     path='C:/Users/CHRIS/web/opa/app/data/covered_calls.csv'
#     df = pd.read_csv(path, encoding='utf-8') if os.path.exists(path) else print("File does not exist")
#     unusual_df = unusual_volume()
#     # Print total records from both files
#     print(f"Total records in covered_calls.csv: {len(df)}")
#     print(f"Total records in unusual_volume.csv: {len(unusual_df)}")
#     # Merge df with unusual_df on 'symbol' to only keep rows that exist in both dataframes
#     merged_df = pd.merge(df, unusual_df[['symbol']], on='symbol', how='inner')
#     # Print matched and unmatched counts
#     print(f"Number of matched records: {len(merged_df)}")
#     print(f"Number of unmatched records in credit_spread.csv: {len(df) - len(merged_df)}")
#     return merged_df

# def dump_data(df,choice='CreditSpreadFile'):
#     '''Creating Pipeline for Database'''
#     # connection_string = f'postgresql://{user_name}:{password}@{host}:{port}/{database_name}'
#     connection_string = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
#     engine = create_engine(connection_string)
#     Session = sessionmaker(bind=engine)
#     if choice == 'CreditSpreadFile':
#         path='C:/Users/CHRIS/web/opa/app/data/credit_spread.csv'
#         df =merged_data()
#         new_columns = [x.replace(" ", "_").replace("/", "_").lower() for x in df.columns]

    
#     df.columns = new_columns
    
#     df['id'] = df.reset_index().index
#     df.rename(columns={'iv_rank': 'rank'}, inplace=True)
#     # df['rank'] = df['rank'].str.replace("%", "").astype(float)
#     # df['prem_width'] = df['prem_width'].str.replace("%", "").astype(float)
#     df['expiry'] = pd.to_datetime(df['expiry'], utc=True)  # Convert 'expiry' column to datetime format
#     df['curr_time'] = pd.to_datetime("now", utc=True)
#     df['days_to_expire'] = (df['expiry'] - df['curr_time']).dt.days
#     df['comment'] = ''
#     df['is_active'] = True
#     df['is_featured'] = True
#     df = df[(df['days_to_expire'] >= 12) & (df['rank'] > 30) & (df['rank'] <= 100) & (df['prem_width'] >= 35) & (df['price'] >= 15)]


#     # Merge df with unusual_df on 'symbol' to only keep rows that exist in both dataframes
#     merged_df = pd.merge(df, unusual_df[['symbol']], on='symbol', how='inner')
    
#     # new_merged_df = merged_df[(merged_df['price'] >= 15)]
#     # merged_df.to_sql('investing_credit_spread', engine, if_exists='replace')
    
#     # print(f"Number of matched records: {len(new_merged_df)}")


# if __name__ == '__main__':
#     unusual_volume()
#     # main_shortput()
#     # main_covered_calls()
#     # send_email()
#     time = datetime.datetime.now()
#     # logger.info(f'Code Executed : {time}')