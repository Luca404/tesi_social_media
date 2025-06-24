import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
import os

#salvo percorsi utili
PATH = Path(__file__).parent
DATA_PATH = PATH/"data"
INDEX_PATH = PATH/".."/"indexes"

#carico api key
load_dotenv( PATH / "keys.env" )
API_KEY = os.getenv( "API_KEY_FMP" )

#scarico market cap mensile di tutti i (tickers_name) per gli ultimi (years) e salvo in (save_name) 
def get_mkt_cap( tickers ):
    output_name = DATA_PATH/f"{INDEX}.csv"

    call_limit = 10    #limite giornaliero
    pause = 1.0 / 4     #max 4 richieste al secondo

    #carico tickers già scaricati
    old_tickers = []
    if output_name.is_file():
        existing_data = pd.read_csv(output_name)
        old_tickers = existing_data["Ticker"].dropna().unique().tolist()
    
    remaining_tickers = [t for t in tickers if t not in old_tickers]    #prendo tutti i ticker dopo quelli già scaricati
    next_tickers = remaining_tickers[:call_limit]                 #riduco a call_limit per limiti API

    mkt_cap = []
    for ticker in next_tickers:
        url = f"https://financialmodelingprep.com/api/v3/historical-market-capitalization/{ticker}"
        params = { "apikey": API_KEY }
        try:
            r = requests.get(url, params=params)
            r.raise_for_status()
            data = r.json()
        
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")

            #trasformo in dati mensili, prendo ultimo giorno del mese
            df.set_index("date", inplace=True)
            monthly = df.resample("ME").last().reset_index()
            monthly["ticker"] = ticker
            mkt_cap.append(monthly[["date", "ticker", "marketCap"]])

        except Exception as e:
            print(f"Errore per {ticker}: {e}")

        time.sleep(pause)
    
    if mkt_cap:
        final_df = pd.concat(mkt_cap, ignore_index=True)
        if old_tickers:
            final_df.to_csv(output_name, mode='a', header=False, index=False)
        else:
            final_df.to_csv(output_name, index=False)
        print(f"Dati salvati correttamente")
    else:
        print("Nessun dato da salvare.")

INDEX = "MS8"

if __name__ == "__main__":
    #carico i tickers
    tickers = pd.read_csv( INDEX_PATH/f"{INDEX}.csv", usecols=["Ticker"] ).iloc[:, 0].dropna().unique().tolist()

    get_mkt_cap( tickers )