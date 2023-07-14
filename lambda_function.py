import requests
import pandas as pd
import datetime
import time
import boto3
from botocore.exceptions import NoCredentialsError
import json
import logging
import subprocess
import sys
import os
import io
from functools import reduce
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import text
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
import botocore
 

subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "/tmp", 'yfinance'])
sys.path.append('/tmp')

import yfinance as yf

def lambda_handler(event, context):
    # Configure logging
    logging.basicConfig(filename='data_ingestion.log', level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
    
    # Define the Alpha Vantage API and API key
    api_key =  os.environ['API'] # Replace with your Alpha Vantage API key
    base_url = os.environ['BASE']
    
    # Define Bank Ticker Symbols that you want to pull
    symbols = ['GS','JPM', 'MS']
    
    # Define the API functions to retrieve data
    functions = ['INCOME_STATEMENT', 'BALANCE_SHEET', 'CASH_FLOW']
    
    # Define FDIC institutions list dataset URL
    fdic_url = os.environ['FDIC']
    
    # Initialize the S3 client
    
    s3 = boto3.client('s3')
    s33 = boto3.resource('s3')
    
    bucket_name = 'bucket.etl' # replace with your bucket name
    
    #Setting up data to perorm calculations
    
    RSSD_list = [1039502,2380443,2162966]
    keep_col = ['RSSD9001','RSSD9999','BHCK2122', 'BHCK3123', 'BHCKS449', 'BHCA3792', 'BHCAA223']
    
    #List to hold quaterly data
    incomelist=[]
    balancelist=[]
    cashflowlist=[]
    
    DELIMITER = "','"
    REGION = "'us-east-1' "
    
           
    #Setting up Stocks table
    try:
        s33.Object(bucket_name, 'stock_data.csv').load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            banks=[[1,'Goldman Sachs Bank','Goldman Sachs Group Inc','GS'],[2,'JPMorgan Chase Bank National Association','JPMorgan Chase & Co','JPM'],[3,'Morgan Stanley Bank National Association','Morgan Stanley','MS']]
            bank_df = pd.DataFrame(banks, columns=['stock_id','bank_name','stock_name','stock_symbol'])
            s3_file_name="stock_data.csv"
            upload_to_aws(bank_df, bucket_name, s3_file_name, s3, s33)
            rs_table="dimension_stock"
            upload_Redshift(rs_table,bucket_name,s3_file_name,DELIMITER,REGION)
        else:
            print("stock data has already been loaded in table")
    
    
    #Uploading csv to s3
    for function in functions:
        for symbol in symbols:
            time.sleep(12) # ensure that you do not exceed the API rate limit
            data = get_data(base_url, api_key, function, symbol)
            if function =='INCOME_STATEMENT':
                incomelist.append(data)
            elif function =='BALANCE_SHEET':
                balancelist.append(data)
            elif function =='CASH_FLOW':
                cashflowlist.append(data)
            if data is not None:
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d')
                s3_file_name = f"{symbol}_{function}_{timestamp}.csv"
                upload_to_aws(data, bucket_name, s3_file_name,s3,s33)
    
    #Uplading combined csv to s3
    dfinc=pd.concat(incomelist)
    dfbal=pd.concat(balancelist)
    dfcash=pd.concat(cashflowlist)
    
    dfa=dfbal.merge(dfcash, left_on='fiscalDateEnding', right_on='fiscalDateEnding')
    dfa=dfa.drop_duplicates()
    
    df_all=dfa.merge(dfinc, left_on='fiscalDateEnding', right_on='fiscalDateEnding')
    df_all=df_all.drop_duplicates()
    
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d')
    combined_name = f"combined-{timestamp}.csv"
    upload_to_aws(df_all, bucket_name, combined_name,s3,s33)
    
    df_all2=df_all[['ticker_x','fiscalDateEnding','netInterestIncome','grossProfit','totalRevenue','incomeTaxExpense','operatingExpenses','investments','longTermInvestments','shortTermInvestments','currentDebt','totalLiabilities','totalShareholderEquity']]
    df_all2.insert(0, 'symbol_id','')
    df_all2.rename(columns = {'ticker_x':'stock_symbol'}, inplace = True)
    df_all2['symbol_id'] = df_all2.apply(f, axis=1)
    df_all2=df_all2.drop_duplicates()
    
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    s3_file_name = f"QUART-{timestamp}.csv"
    upload_to_aws(df_all2, bucket_name, s3_file_name,s3,s33)
    rs_table="dimension_quarterly"
    upload_Redshift(rs_table,bucket_name,s3_file_name,DELIMITER,REGION)
    
    RSSD_list = [1039502,2380443,2162966]
    keep_col = ['RSSD9001','RSSD9999','BHCK2122', 'BHCK3123', 'BHCKS449', 'BHCA3792', 'BHCAA223']

    # calc ROE, ROA, NIM
    df_ratio = pd.DataFrame()
    
    for symbol in symbols:
        df_temp = calc_ratio(df_all)
        df_temp['Ticker'] = symbol
        df_ratio = pd.concat([df_ratio, df_temp])
    
    # calc NPL, CAR
    file_name='all.csv'
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    df = pd.read_csv(obj['Body'])
    df = df[df['RSSD9001'].isin(RSSD_list)]
    ticker_RSSD_mapping = pd.DataFrame({'Ticker': symbols, 'RSSD': RSSD_list})
    df_ratio_2 = cal_ratio_2(df)
    
    # convert RSSD to ticker
    df_ratio_2 = df_ratio_2.merge(ticker_RSSD_mapping, left_on = 'RSSD9001', right_on = 'RSSD')
    df_ratio_2 = df_ratio_2[['RSSD9999','Ticker','NPL','CAR']]
    df_ratio_2 = df_ratio_2.rename(columns = {'RSSD9999':'fiscalDateEnding'})
    
    # combine the two dataframe for ratios
    df_ratio_2['fiscalDateEnding'] = pd.to_datetime(df_ratio_2['fiscalDateEnding'].astype('str'), errors='coerce',format = '%Y%m%d').astype('str')
    df_ratio = df_ratio_2.merge(df_ratio, how = 'left', left_on = ['fiscalDateEnding','Ticker'], right_on = ['fiscalDateEnding','Ticker'])
    df_ratio=df_ratio[['Ticker','fiscalDateEnding','NPL','CAR','ROE','ROA','NIM']]
    df_ratio.fillna(0,inplace=True)
    df_ratio.insert(0, 'symbol_id','')
    df_ratio.rename(columns = {'Ticker':'stock_symbol'}, inplace = True)
    df_ratio['symbol_id'] = df_ratio.apply(f, axis=1)
    df_ratio=df_ratio.drop_duplicates()
    
    if not df_ratio.empty:
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        s3_file_name = f"ratio-{timestamp}.csv"
        upload_to_aws(df_ratio, bucket_name, s3_file_name, s3, s33)
        rs_table="dimension_ratios"
        upload_Redshift(rs_table,bucket_name,s3_file_name,DELIMITER,REGION)
    
    
    # Fetch historical stock data
    for symbol in symbols:
        stocks_data = fetch_historical_stock_data_yf(symbol)
        if not stocks_data.empty:
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            s3_file_name = f"{symbol}-Historical_Stock_Data_{timestamp}.csv"
            upload_to_aws(stocks_data, bucket_name, s3_file_name,s3,s33)

    # Fetch FDIC data
    fdic_data_dict = fetch_fdic_dataset(fdic_url)
    loc=fdic_data_dict[['NAME','ADDRESS','CITY','COUNTY','STNAME','ZIP']]
    loc = loc.replace(',','', regex=True)  

    if not fdic_data_dict.empty:
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        s3_file_name = f"loc-{timestamp}.csv"
        upload_to_aws(loc, bucket_name, s3_file_name, s3, s33)
        rs_table="dimension_location"
        upload_Redshift(rs_table,bucket_name,s3_file_name,DELIMITER,REGION)
        
        fdic_data_dict = fetch_fdic_dataset(fdic_url)
        comp=fdic_data_dict[['NAME','OFFDOM','OFFFOR','OFFOA','NAMEHCR','INSFDIC','ASSET','DEP','EQ','NETINC','ROA','ROE','ROAQ','ROEQ']]
        comp.fillna(0,inplace=True)
        comp['OFFDOM']=comp['OFFDOM'].astype(int)
        comp['OFFFOR']=comp['OFFFOR'].astype(int)
        comp['OFFOA']=comp['OFFOA'].astype(int)
        comp['INSFDIC']=comp['INSFDIC'].astype(int)
        comp['ASSET']=comp['ASSET'].astype(int)
        comp['DEP']=comp['DEP'].astype(int)
        comp['EQ']=comp['EQ'].astype(int)
        comp['NETINC']=comp['NETINC'].astype(int)
        comp = comp.replace(',','', regex=True)  
        s3_file_name = f"comp-{timestamp}.csv"
        upload_to_aws(comp, bucket_name, s3_file_name, s3, s33)
        rs_table="dimension_company"
        upload_Redshift(rs_table,bucket_name,s3_file_name,DELIMITER,REGION)
   

   
    #Settig up Time table

    try:
        s33.Object(bucket_name, 'time.csv').load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            url=os.environ['TIME']
            time_data = pd.read_csv(url,
                               parse_dates=['date']
                              )
            s3_file_name="time.csv"
            upload_to_aws(time_data, bucket_name, s3_file_name, s3, s33)
            rs_table="dimension_time"
            upload_Redshift(rs_table,bucket_name,s3_file_name,DELIMITER,REGION)
        else:
            print("stock data has already been loaded in table")
    

def upload_to_aws(data, bucket, s3_file, s3, s33):
    try:
        csv_buffer = io.StringIO()
        data.to_csv(csv_buffer,index=False)
        #s3.put_object(Body=json.dumps(data), Bucket=bucket, Key=s3_file)
        s33.Object(bucket, s3_file).put(Body=csv_buffer.getvalue())
        logging.info(f"Upload Successful for {s3_file}")
        return True
    except NoCredentialsError:
        logging.error("No AWS Credentials provided")
        return False

def get_data(base_url, api_key, function, symbol):
    params = {
        'function': function,
        'symbol': symbol,
        'apikey': api_key,
    }

    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
       res = response.json()
       df = pd.DataFrame(res['quarterlyReports'])
       df.insert(loc=0, column='ticker', value=symbol)
       return df
    else:
        logging.error(f"Failed to fetch data for {symbol} using {function}")
        return None

def fetch_historical_stock_data_yf(symbol):
    stock = yf.Ticker(symbol)
    hist_data = stock.history(start="2017-01-01", end="2022-03-31")
    
    if hist_data.empty:
        logging.error(f"No historical data available for {symbol}")
        
    df = pd.DataFrame( hist_data.to_dict(orient='records'))                             
    df.insert(loc=0,column='ticker',value=symbol)       
    return df

def fetch_fdic_dataset(fdic_url):
    try:
        fdic_data_dict = pd.read_csv(fdic_url)
        #fdic_data_dict = fdic_data.to_dict(orient='records')

        return fdic_data_dict
    except Exception as e:
        logging.error(f"Failed to fetch FDIC dataset: {e}")
        return None
        
def upload_Redshift(rs_table,bucket_name,file,DELIMITER,REGION):
    url2 = URL.create(
            drivername='redshift+psycopg2',#'redshift+redshift_connector', # indicate redshift_connector driver and dialect will be used
            host=os.environ['host'], # Amazon Redshift
            port=5439, # Amazon Redshift port
            database="dev", # Amazon Redshift database
            username=os.environ['user'], # Amazon Redshift username
            password=os.environ['password'] # Amazon Redshift password
            )
    
    print(url2)
    engine = sa.create_engine(url2)
    role=os.environ['ROLE']
    
    db = scoped_session(sessionmaker(bind=engine))
    
    copy_query = "COPY "+rs_table+" from 's3://"+ bucket_name+'/'+file+"' iam_role '"+role+"' delimiter "+DELIMITER+" IGNOREHEADER 1 REGION " + REGION
    db.execute(text(copy_query))
    db.commit()
    db.close()

def calc_ratio(df_all):
    df_all['ROE'] = (df_all['netIncome_x'].astype(int)/df_all['totalShareholderEquity'].astype(int))*4
    df_all['ROA'] = (df_all['netIncome_x'].astype(int)/df_all['totalAssets'].astype(int))*4
    df_all['NIM'] = (df_all['netIncome_x'].astype(int)/df_all['totalAssets'].astype(int))*4
    df_ratio = df_all[['fiscalDateEnding', 'ROE', 'ROA',  'NIM']]
    return df_ratio

def cal_ratio_2(df):# cal ratio
    try:
        df['NPL'] = df['BHCKS449']/df['BHCK2122']
    except:
        df['NPL'] = 0
    try:
        df['CAR'] = df['BHCA3792']/df['BHCAA223']
    except:
        df['CAR'] = 0
    df_ratio_2 = df[['RSSD9001','RSSD9999','NPL','CAR']]
    return df_ratio_2

def f(row):
    if row['stock_symbol'] == 'GS':
        val = 1
    elif row['stock_symbol'] == 'JPM':
        val = 2
    elif row['stock_symbol'] == 'MS':
        val = 3
    return val

    
    
