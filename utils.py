import pandas as pd
import yfinance as yf

def fetch_data_util(ticker_symbol):
    ticker_data = yf.Ticker(ticker_symbol)
    # Fetch key statistics and then extract only valuation measures
    full_data = ticker_data.info
    ''' ============== All financial terms =============   '''
    valuation_keys = ["overallRisk", "sharesShort", "enterpriseToEbitda", "ebitda", "quickRatio", "currentRatio", "revenueGrowth"]
    data = {key: full_data.get(key, None) for key in valuation_keys}
    return data

def unusual_volume():
    # path='C:/Users/CHRIS/web/opa/app/data/unusual_volume.csv'
    # udf = pd.read_csv(path, encoding='utf-8') if os.path.exists(path) else print("File does not exist")
    udf = pd.read_csv('credit_spread.csv')
    new_columns = [x.replace(" ", "_").replace("/", "_").lower() for x in udf.columns]
    udf.columns = new_columns
    udf['price'] = udf['price'].astype(float)
    udf['volume'] = udf['volume'].astype(float)

  # Fetch EBITDA for each symbol in unusual_df
    udf['ebitda'] = udf['symbol'].apply(lambda x: fetch_data_util(x).get('ebitda', None))
    
    # Filter out symbols where EBITDA <= 0
    positive_ebitda_df = udf[udf['ebitda'] > 0]

    filtered_df = positive_ebitda_df[(positive_ebitda_df['price'] >= 15) & (positive_ebitda_df['volume'] > 1000)]

    return filtered_df


