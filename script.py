import requests
import os
import csv
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
print("Hello world")

limit = 10

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

    #print(len(tickers))

    # Write tickers to CSV file
    csv_filename = 'tickers.csv'
    if tickers:
        # Get the fieldnames from the first ticker (they should all have the same schema)
        fieldnames = tickers[0].keys()
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(tickers)
        
        print(f"Successfully wrote {len(tickers)} tickers to {csv_filename}")
    else:
        print("No tickers to write to CSV")

# Call the function to run the stock job
if __name__ == "__main__":
    run_stock_job()

#example_ticker =  {'ticker': 'BASE', 'name': 'Couchbase, Inc. Common Stock', 'market': 'stocks', 'locale': 'us', 'primary_exchange': 'XNAS', 'type': 'CS', 'active': True, 'currency_name': 'usd', 'cik': '0001845022', 'composite_figi': 'BBG001Z5ZB04', 'share_class_figi': 'BBG001Z5ZB22', 'last_updated_utc': '2025-09-17T15:54:25.561651054Z'}