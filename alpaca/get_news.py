from alpaca.data.historical import NewsClient
from alpaca.data.requests import NewsRequest
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import os
import time

#salvo percorso assoluto del file
PATH = Path(__file__).parent
DATA_PATH = PATH / "data"

#carico api key
load_dotenv( PATH / "keys.env" )
API_KEY = os.getenv( "KEY" )
SECRET_KEY = os.getenv( "SECRET" )


def get_news( ticker, months ):
    try:
        client = NewsClient(api_key=API_KEY, secret_key=SECRET_KEY)
    except Exception as e:
        print(f"Errore durante l'inizializzazione del client Alpaca: {e}")
        exit(1)
        
        
    #Calcolo date
    today = datetime.today()
    start = (today - timedelta(days=months*30)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")

    request = NewsRequest(symbols=ticker, start=start, end=end)
    news = client.get_news(request)
    rows = []
    for article in news["news"]:
        rows.append({
            "symbol": ticker,
            "timestamp": article.created_at,
            "title": article.headline,
            "summary": article.summary,
            "source": article.source,
            "url": article.url
        })
    
    df = pd.DataFrame(rows)

    # Salva in CSV
    df.to_csv( DATA_PATH / f"news_{ticker}.csv", index=False)
    print(f"Salvate {len(df)} notizie in news_{ticker}.csv")


if __name__ == "__main__":
    #scarico news relative a (ticker), per gli scorsi (mesi)
    get_news( "AAPL", 1 )
    