import requests
import os
import csv
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# Snowflake connection parameters
SNOWFLAKE_ACCOUNT= os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER= os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD= os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE= os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE= os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA= os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE= os.getenv("SNOWFLAKE_TABLE")

print("Hello world")

limit = 10
ds = datetime.now().strftime("%Y-%m-%d")

def get_snowflake_connection():
    """Create and return a Snowflake connection"""
    try:
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        
        # Set the current database and schema
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
        cursor.close()
        
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

def create_tickers_table(conn):
    """Create the tickers table if it doesn't exist"""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
            ticker VARCHAR,
            name VARCHAR,
            market VARCHAR,
            locale VARCHAR,
            primary_exchange VARCHAR,
            type VARCHAR,
            active BOOLEAN,
            currency_name VARCHAR,
            cik VARCHAR,
            composite_figi VARCHAR,
            share_class_figi VARCHAR,
            last_updated_utc TIMESTAMP_TZ,
            ds DATE
    )
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        cursor.close()
        print(f"Table {SNOWFLAKE_TABLE} created or already exists")
    except Exception as e:
        print(f"Error creating table: {e}")

def insert_tickers_to_snowflake(tickers):
    """Insert ticker data into Snowflake table"""
    if not tickers:
        print("No tickers to insert")
        return
    
    conn = get_snowflake_connection()
    if not conn:
        return
    
    try:
        # Create table if it doesn't exist
        create_tickers_table(conn)
        
        # Prepare insert statement using Snowflake's parameter binding
        insert_sql = f"""
        INSERT INTO {SNOWFLAKE_TABLE} 
        (ticker, name, market, locale, primary_exchange, type, active, 
         currency_name, cik, composite_figi, share_class_figi, last_updated_utc, ds)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor = conn.cursor()
        
        # Prepare data for insertion
        insert_data = []
        for ticker in tickers:
            row = (
                ticker.get('ticker'),
                ticker.get('name'),
                ticker.get('market'),
                ticker.get('locale'),
                ticker.get('primary_exchange'),
                ticker.get('type'),
                ticker.get('active'),
                ticker.get('currency_name'),
                ticker.get('cik'),
                ticker.get('composite_figi'),
                ticker.get('share_class_figi'),
                ticker.get('last_updated_utc'),
                ds
            )
            insert_data.append(row)
        
        # Execute batch insert
        cursor.executemany(insert_sql, insert_data)
        conn.commit()
        
        print(f"Successfully inserted {len(tickers)} tickers into Snowflake table {SNOWFLAKE_TABLE}")
        
    except Exception as e:
        print(f"Error inserting data into Snowflake: {e}")
    finally:
        if conn:
            conn.close()

def run_stock_job():
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={limit}&sort=ticker&apiKey={POLYGON_API_KEY}'

    #print(url)

    response = requests.get(url)
    #print(response.json())

    tickers = []

    data = response.json()
    print(data.keys())

    # Check if the response contains results, if not it might be an error
    if 'results' in data:
        #print(data["results"])
        for t in data['results']:
            tickers.append(t)
    else:
        print(f"Error in initial response: {data}")
        exit(1)

    #print(len(tickers))   

    while 'next_url' in data:
        print('requesting next page '+ data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        print(data.keys())
        
        # Check if the response contains results, if not it might be an error
        if 'results' in data:
            for t in data['results']:
                tickers.append(t)
        else:
            print(f"Error in response: {data}")
            break

    print(f"Collected {len(tickers)} tickers")

    # Insert tickers into Snowflake table
    insert_tickers_to_snowflake(tickers)

# Call the function to run the stock job
if __name__ == "__main__":
    run_stock_job()

#example_ticker =  {'ticker': 'BASE', 'name': 'Couchbase, Inc. Common Stock', 'market': 'stocks', 'locale': 'us', 'primary_exchange': 'XNAS', 'type': 'CS', 'active': True, 'currency_name': 'usd', 'cik': '0001845022', 'composite_figi': 'BBG001Z5ZB04', 'share_class_figi': 'BBG001Z5ZB22', 'last_updated_utc': '2025-09-17T15:54:25.561651054Z'}