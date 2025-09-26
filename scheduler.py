import schedule
import time
from script import run_stock_job

from datetime import datetime


def basic_job():
    print("Job started at: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
#run every 1 minute
schedule.every().minutes.do(basic_job)
#run every 1 minute
schedule.every().minutes.do(run_stock_job)

while True:
    schedule.run_pending()
    time.sleep(1)