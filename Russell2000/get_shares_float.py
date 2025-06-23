import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
import os

#salvo percorso assoluto del file
PATH = Path(__file__).parent
DATA_PATH = PATH / "data"

#carico api key
load_dotenv( PATH / "keys.env" )
API_KEY = os.getenv( "API_KEY_FMP" )

#scarico market cap mensile di tutti i (tickers_name) per gli ultimi (years) e salvo in (save_name) 
def get_float( tickers, years, save_name ):
    output_name = DATA_PATH / save_name

    call_limit = 250    #limite giornaliero
    pause = 1.0 / 4     #max 4 richieste al secondo

    #Calcolo date
    today = datetime.today()
    start = (today - timedelta(days=years*365)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")

    #carico tickers già scaricati
    old_tickers = []
    if os.path.exists( output_name ):
        existing_data = pd.read_csv(output_name)
        old_tickers = existing_data["ticker"].unique().tolist()
    
    remaining_tickers = [t for t in tickers if t not in old_tickers]    #prendo tutti i ticker dopo quelli già scaricati
    next_tickers = remaining_tickers[:call_limit]                 #riduco a call_limit per limiti API

    floats = []
    for ticker in next_tickers:
        url = f"https://financialmodelingprep.com/api/v4/historical/shares_float"
        params = { "symbol": ticker, "apikey": API_KEY }
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
            floats.append(monthly[["ticker", "date", "marketCap"]])

        except Exception as e:
            print(f"Errore per {ticker}: {e}")

        time.sleep(pause)
    
    if floats:
        final_df = pd.concat(floats, ignore_index=True)
        if old_tickers:
            final_df.to_csv(output_name, mode='a', header=False, index=False)
        else:
            final_df.to_csv(output_name, index=False)
        print(f"Dati salvati in {save_name}")
    else:
        print("Nessun dato da salvare.")


if __name__ == "__main__":
    tickers_name = "R2000_2025_tickers.csv"
    #carico tutti i tickers
    tickers = pd.read_csv( DATA_PATH / tickers_name, usecols=["Ticker"] ).iloc[:, 0].tolist()

    get_float( tickers, 1, "R2000_2025_float.csv" )