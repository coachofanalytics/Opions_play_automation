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
from utils import merged_data,read_data_from_csv,process_data

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

vl_merged_df = merged_data()

def dump_data(df, choice,vl_merged_df):
    '''Creating Pipeline for Database'''
    connection_string = f'postgresql://{user_name}:{password}@{host}:{port}/{database_name}'
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    vl_merged_df = merged_data()
    
    if choice == 'CreditSpreadFile':
        csv_file_path  = 'credit_spread.csv'
        try:
            df=read_data_from_csv(csv_file_path)[0]
        except:
            print('No Report is available')
        merged_df=process_data(df,vl_merged_df)
        merged_df.to_sql('investing_credit_spread', engine, if_exists='replace', index=False)

    elif choice == 'coveredCalls':
        csv_file_path  = 'covered_calls.csv'
        try:
            df=read_data_from_csv(csv_file_path)[0]
        except:
            print('No Report is available')
        merged_df=process_data(df,vl_merged_df)
        merged_df.to_sql('investing_covered_calls', engine, if_exists='replace', index=False)

    else:
        csv_file_path  = 'shortput.csv'
        try:
            df=read_data_from_csv(csv_file_path)[0]
        except:
            print('No Report is available')
        merged_df=process_data(df,vl_merged_df)
        merged_df.to_sql('investing_shortput', engine, if_exists='replace', index=False)

# def dump_data(df, choice,vl_merged_df):
#     '''Creating Pipeline for Database'''
#     connection_string = f'postgresql://{user_name}:{password}@{host}:{port}/{database_name}'
#     engine = create_engine(connection_string)
#     Session = sessionmaker(bind=engine)
    
#     if choice == 'CreditSpreadFile':
#         # df = pd.read_csv('credit_spread.csv')
#         csv_file_path = 'credit_spread.csv'
#         try:
#             df=read_data_from_csv(csv_file_path)[0]
#         except:
#             print('No Report is available')
#         new_columns = [x.replace(" ", "_").replace("/", "_").lower() for x in df.columns]
#         df.columns = new_columns
#         df.rename(columns={'iv_rank': 'rank'}, inplace=True)
#         df['rank'] = df['rank'].str.replace("%", "").astype(float)
#         df['price'] = df['price'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).astype(float)

#         # Replace old records with new data in the database table
#         merged_df.to_sql('investing_credit_spread', engine, if_exists='replace', index=False)
        
#     elif choice == 'coveredCalls':
#         df = pd.read_csv('covered_calls.csv')
#         new_columns = [x.replace(" ", "_").replace("/", "_").lower() for x in df.columns]
#         df['stock_price'] = df['stock_price'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).astype(float)
#         df['expiry'] = pd.to_datetime(df['expiry'], utc=True)  # Convert 'expiry' column to datetime format
#         df['comment'] = 'comment'
#         df['on_date'] = ' '
#         df['is_active'] = True
#         df['is_featured'] = True
#         merged_df = pd.merge(df, vl_merged_df[['symbol']], on='symbol', how='inner')
#         # Generate unique IDs for the new data in merged_df
#         merged_df['id'] = range(1, len(merged_df) + 1)

#         # Replace old records with new data in the database table
#         merged_df.to_sql('investing_covered_calls', engine, if_exists='replace', index=False)
#     else:
#         df = pd.read_csv('shortput.csv')
#         new_columns = [x.lower().replace(" ", "_").replace("/", "_") for x in df.columns]
#         df.columns = new_columns
#         df['stock_price'] = df['stock_price'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).astype(float)
#         df['expiry'] = pd.to_datetime(df['expiry'], utc=True)
#         df['comment'] = ' '
#         df['on_date'] = ' '
#         df['is_active'] = True
#         df['is_featured'] = True

#         merged_df = pd.merge(df, vl_merged_df[['symbol']], on='symbol', how='inner')

#         # Generate unique IDs for the new data in merged_df
#         merged_df['id'] = range(1, len(merged_df) + 1)

#         # Replace old records with new data in the database table
#         merged_df.to_sql('investing_shortput', engine, if_exists='replace', index=False)



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

        
