import logging
import logging.handlers
import os
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import subprocess
import pandas as pd
import datetime
import psycopg2
from utils import merged_data,oversold_overbought

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)

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

def dump_data(df, choice):
    '''Creating Pipeline for Database'''
    connection_string = f'postgresql://{user_name}:{password}@{host}:{port}/{database_name}'
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)

    if choice == 'CreditSpreadFile':
        df = pd.read_csv('credit_spread.csv')
        new_columns = [x.replace(" ", "_").replace("/", "_").lower() for x in df.columns]
        df.columns = new_columns
        # df['id'] = df.reset_index().index
        df.rename(columns={'iv_rank': 'rank'}, inplace=True)
        df['rank'] = df['rank'].str.replace("%", "").astype(float)
        df['prem_width'] = df['prem_width'].str.replace("%", "").astype(float)
        df['price'] = df['price'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).astype(float)
    
        df['expiry'] = pd.to_datetime(df['expiry'], utc=True)  # Convert 'expiry' column to datetime format
        df['curr_time'] = pd.to_datetime("now", utc=True)
    
        df['days_to_expire'] = (df['expiry'] - df['curr_time']).dt.days
        df['days_to_expire'] = df['days_to_expire'].abs()
        df['comment'] = ' '
        df['on_date'] = ' '
        df['is_active'] = True
        df['is_featured'] = True

        # Fetch the liquidity_unusualvolume merged dataframe
        columns_to_keep =['symbol','strategy','type','price','sell_strike','buy_strike','expiry','premium','width','prem_width','rank','earnings_date','comment','on_date','is_active','is_featured']
        vl_merged_df = merged_data()
        try:
            # Merge df with vl_merged_df on 'symbol' to only keep rows that exist in both dataframes
            # merged_df = pd.merge(df, vl_merged_df, on='symbol', how='inner')
            merged_df = pd.merge(df[columns_to_keep], vl_merged_df[['symbol']], on='symbol', how='inner')

        except:
            print('No data')

        # Fetch the overbought oversold merged dataframe
        osb_df = oversold_overbought()
        fm_df = pd.merge(merged_df, osb_df, on='symbol', how='inner')
        # final_merged_df = pd.merge(merged_df,osb_df[['symbol']], on='symbol', how='inner')

        # df = df[(df['days_to_expire'] >= 12) & (df['rank'] > 30) & (df['rank'] <= 100) & (df['prem_width'] >= 35) & (df['price'] >= 15)]
        # filtered_df = df[(df['rank'] > 15) & (df['rank'] <= 75) & (df['price'] >= 15)]
        
        # Generate unique IDs for the new data in merged_df
        fm_df['id'] = range(1, len(fm_df) + 1)

        # merged_df.to_sql('investing_credit_spread', engine, if_exists='append', index=False)
        # Replace old records with new data in the database table
        fm_df.to_sql('investing_credit_spread', engine, if_exists='replace', index=False)
        
    elif choice == 'coveredCalls':
        df = pd.read_csv('covered_calls.csv')
        new_columns = [x.replace(" ", "_").replace("/", "_").lower() for x in df.columns]
        df.columns = new_columns
        # df['id'] = df.reset_index().index
        df['implied_volatility_rank'] = df['implied_volatility_rank'].str.replace('%', '').astype('float')
        df.rename(columns={'implied_volatility_rank': 'rank'}, inplace=True)
        df['raw_return'] = df['raw_return'].str.replace('%', '').astype('float')
        df['annualized_return'] = df['annualized_return'].str.replace('%', '').str.replace('∞', '0').astype('float')
        df['stock_price'] = df['stock_price'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).astype(float)
    
        df['expiry'] = pd.to_datetime(df['expiry'], utc=True)  # Convert 'expiry' column to datetime format
        df['curr_time'] = pd.to_datetime("now", utc=True)
    
        df['days_to_expire'] = (df['expiry'] - df['curr_time']).dt.days
        df['days_to_expire'] = df['days_to_expire'].abs()
        df['comment'] = 'comment'
        df['on_date'] = ' '
        df['is_active'] = True
        df['is_featured'] = True

        # Fetch the liquidity_unusualvolume merged dataframe
        vl_merged_df = merged_data()
        columns_to_keep=['symbol','action','expiry','days_to_expiry','strike_price','mid_price','bid_price','ask_price','implied_volatility_rank','earnings_date','earnings_flag','stock_price','raw_return','annualized_return','distance_to_strike','comment','on_date','is_active','is_featured']
        try:
            # Merge df with vl_merged_df on 'symbol' to only keep rows that exist in both dataframes
            # merged_df = pd.merge(df, vl_merged_df, on='symbol', how='inner')
            merged_df = pd.merge(df[columns_to_keep], vl_merged_df[['symbol']], on='symbol', how='inner')
        except:
            print('No data')
            
        # Apply the filter rules
        # df=merged_df
        # df = df[(df['days_to_expire'] >= 21) & (df['implied_volatility_rank'] > 4) & (df['raw_return'] >= 3.5) & (df['stock_price'] >= 15)]
        # filtered_df = df [(df['days_to_expire'] >= 21) & (df['rank'] <= 65)]
        
        # Fetch the overbought oversold merged dataframe
        osb_df = oversold_overbought()
        fm_df = pd.merge(merged_df, osb_df, on='symbol', how='inner')
        # final_merged_df = pd.merge(merged_df,osb_df[['symbol']], on='symbol', how='inner')

        # Generate unique IDs for the new data in merged_df
        fm_df['id'] = range(1, len(fm_df) + 1)

        # Replace old records with new data in the database table
        fm_df.to_sql('investing_covered_calls', engine, if_exists='replace', index=False)
        
        
    else:
        df = pd.read_csv('shortput.csv')
        new_columns = [x.lower().replace(" ", "_").replace("/", "_") for x in df.columns]
        df.columns = new_columns
        # df['id'] = df.reset_index().index
        df['implied_volatility_rank'] = df['implied_volatility_rank'].str.replace('%', '').astype('float')
        df['raw_return'] = df['raw_return'].str.replace('%', '').astype('float')
        df['annualized_return'] = df['annualized_return'].str.replace('%', '').str.replace('∞', '0').astype('float')
        df['stock_price'] = df['stock_price'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).astype(float)
        df['expiry'] = pd.to_datetime(df['expiry'], utc=True)
        df['curr_time'] = pd.to_datetime("now", utc=True)
        
        df['days_to_expire'] = (df['expiry'] - df['curr_time']).dt.days
        df['days_to_expire'] = df['days_to_expire'].abs()
        
        df['comment'] = ' '
        df['on_date'] = ' '
        df['is_active'] = True
        df['is_featured'] = True
        
        columns_to_keep=['symbol','action','expiry','days_to_expiry','strike_price','earnings_date','stock_price','raw_return','distance_to_strike','comment','on_date','is_active','is_featured']
        # Fetch the liquidity_unusualvolume merged dataframe
        vl_merged_df = merged_data()
        try:
            # Merge df with vl_merged_df on 'symbol' to only keep rows that exist in both dataframes
            merged_df = pd.merge(df, vl_merged_df[['symbol']], on='symbol', how='inner')
        except:
            # Merge df with unusual_df on 'symbol' to only keep rows that exist in both dataframes
            merged_df = df
            
        # Fetch the overbought oversold merged dataframe
        osb_df = oversold_overbought()
        final_merged_df = pd.merge(merged_df,osb_df[['symbol']], on='symbol', how='inner')

        # df = df[(df['days_to_expire'] >= 21) & (df['implied_volatility_rank'] > 50) & (df['implied_volatility_rank'] <= 100) & (df['annualized_return'] >= 65) & (df['stock_price'] > 15)]
        # filtered_df = df [(df['days_to_expire'] >= 25) & (df['implied_volatility_rank'] > 15) & (df['implied_volatility_rank'] <= 75) & (df['annualized_return'] >= 45) ]
        
        # Generate unique IDs for the new data in merged_df
        final_merged_df['id'] = range(1, len(final_merged_df) + 1)
        final_merged_df.to_sql('investing_shortput', engine, if_exists='replace', index=False)
        
def parse_data(html, choice):
    '''Extract the data table'''
    result = subprocess.run(["playwright", "install"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.select(f'table#{choice}')[0]
    columns = table.find('thead').find_all('th')
    df = pd.read_html(str(table))[0]
    df = df.iloc[:-1,:]
    
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str[:255]
    return df
    
def extract_data(url, choice):
    '''Extract the HTML code'''
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, slow_mo=50, chromium_sandbox = False)
        page = browser.new_page()
        
        page.goto(url=url)

        page.fill(
           'input#Login', USERNAME
        )
        page.fill(
            'input#Password', KEY
        )

        #click on submit button
        page.click("button[type=submit]")

        # Wait to load the HTML template
        try:
            page.wait_for_timeout(30000)
        except:
            page.wait_for_timeout(60000)

        if choice == 'CreditSpreadFile':
            html = page.inner_html('//*[@id="CreditSpreadFile_wrapper"]')

        elif choice == 'coveredCalls':
            html = page.inner_html('//*[@id="coveredCalls_wrapper"]')

        else:
            html = page.inner_html('//*[@id="shortPuts_wrapper"]')
        return html

#Executing the options play tables
def main_covered_calls():
    # print("COVERED CALLS")
    try:
        urls = {
            'coveredCalls' : 'https://www.optionsplay.com/hub/covered-calls'
        }

        html = extract_data(url=urls['coveredCalls'], choice='coveredCalls')
        data = parse_data(html, choice='coveredCalls')
        data.to_csv('covered_calls.csv', index=False)
        dump_data(df=data, choice='coveredCalls')
    except Exception as err:
        print(err)
    
    

def main_shortput():
    print("SHORTPUT")
    try:
        urls = {
            'shortPuts' : 'https://www.optionsplay.com/hub/short-puts'
        }

        html = extract_data(url=urls['shortPuts'], choice='shortPuts')
        data = parse_data(html, choice='shortPuts')
        data.to_csv('shortput.csv', index=False)
        dump_data(df=data, choice='shortPuts')
    except Exception as err:
        print(err)

def main_cread_spread():
    print("CREDITSPREAD")
    try:
        urls = {
            'CreditSpreadFile' : 'https://www.optionsplay.com/hub/credit-spread-file'
        }

        html = extract_data(url=urls['CreditSpreadFile'], choice='CreditSpreadFile')
        data = parse_data(html, choice='CreditSpreadFile')  
        data.to_csv('credit_spread.csv', index=False)
        dump_data(df=data, choice='CreditSpreadFile') 
    except Exception as err:
        print(err)


if __name__ == '__main__':
    main_cread_spread()
    main_shortput()
    main_covered_calls()
    # send_email()
    time = datetime.datetime.now()
    logger.info(f'Code Executed : {time}')

        
