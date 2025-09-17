import re
from unittest import result
import requests
import os
from dotenv import load_dotenv
from urllib3 import response
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
print("Hello world")

url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit=100&sort=ticker&apiKey={POLYGON_API_KEY}'

response = requests.get(url)
#print(response.json())

tickers = []


data = response.json()
print(data.keys())

for ticker in data['results']:
    tickers.append(ticker)

print(tickers)    