from pathlib import Path
import pandas as pd
from pytrends.request import TrendReq
import time


def get_google_trends(tickers, tf):
    pytrends = TrendReq(hl='en-US', tz=360)
    data = []
    for ticker in tickers:
        try:
            #richiesta, cat=categoria, geo=stato, gprop=tipo (news, youtube, images, ...)
            pytrends.build_payload([ticker], cat=0, timeframe=tf, geo="US", gprop="")
            interest = pytrends.interest_over_time().infer_objects(copy=False)
            
            if not interest.empty:
                interest = interest[[ticker]]
                data.append(interest)
            
            time.sleep(1)  #rispetta i limiti di Google

        except Exception as e:
            print(f"Errore con {ticker}: {e}")

    #se ci sono dati concateno e salvo
    if data:
        df = pd.concat(data, axis=1)
        df.to_csv(DATA_PATH/f"{INDEX}_trends.csv")
        print("Salvataggio completato")
    else:
        print("Nessun dato disponibile")


PATH = Path(__file__).parent
DATA_PATH = PATH/"data"
INDEX_PATH = PATH/".."/"indexes"

INDEX = "MS8"

if __name__ == "__main__":
    #carico i tickers
    tickers = pd.read_csv( INDEX_PATH/f"{INDEX}.csv", usecols=["Ticker"] ).iloc[:, 0].dropna().unique().tolist()

    timeframe = "today 5-y"
    get_google_trends(tickers, timeframe)