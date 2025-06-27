from pathlib import Path
import pandas as pd
import yfinance as yf
import time


PATH = Path(__file__).parent
DATA_PATH = PATH/"data"
INDEX_PATH = PATH/".."/"indexes"

INDEX = "MS8"

if __name__ == "__main__":
    #carico tickers
    tickers = pd.read_csv( INDEX_PATH/f"{INDEX}.csv", usecols=["Ticker"] ).iloc[:, 0].dropna().unique().tolist()

    #data di inizio e fine
    start_date = "2020-01-01"
    end_date = "2025-06-22"

    #scarico dati daily per tutti i ticker
    data = yf.download(tickers, start=start_date, end=end_date, interval="1d", group_by='ticker', auto_adjust=True)

    #salvo una copia pulita solo con Adj Close
    adj_close = pd.DataFrame({
        ticker: data[ticker]["Close"] for ticker in tickers
    })

    adj_close = adj_close.reset_index()
    adj_close.to_csv(DATA_PATH/f"{INDEX}_prices.csv", index=False)

    print(adj_close.head())
