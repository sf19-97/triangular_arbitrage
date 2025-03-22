import schedule
import time
from fetch_oanda import fetch_oanda_rates
from matrix_builder import build_exchange_matrix

def job_fetch_and_build():
    # Example list of instruments
    instruments = ["EUR_USD","USD_JPY","GBP_USD","USD_CHF","USD_CAD","AUD_USD","NZD_USD"]
    currencies = ["USD","EUR","JPY","GBP","CHF","CAD","AUD","NZD"]

    rate_dict = fetch_oanda_rates(instruments)
    M = build_exchange_matrix(rate_dict, currencies, base_currency="USD")
    print("Updated Exchange Matrix:")
    print(M)

def run_scheduler():
    # Fetch every minute, for example
    schedule.every(1).minutes.do(job_fetch_and_build)

    while True:
        schedule.run_pending()
        time.sleep(1)
