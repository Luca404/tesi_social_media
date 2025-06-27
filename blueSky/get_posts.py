from atproto import Client
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, date, timedelta
import os
import re
import time
import pytz

#salvo percorso assoluto del file
PATH = Path(__file__).parent

#carico credenziali
load_dotenv( PATH / "keys.env" )
USERNAME = os.getenv( "USERN" )
PASSWORD = os.getenv( "PASSW" )

#uniformare la data (created_at)
def standardize_date(raw_date):
    #parse la data con fuso orario
    dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
    #converte in UTC
    dt_utc = dt.astimezone(pytz.utc)
    #formatta la data in formato ISO con 3 decimali (millisecondi)
    uniform_date = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return uniform_date

def get_posts( ticker, keys, years ):
    #imposto date finale
    end_date = date.today()

    #carico post già presenti
    existing_ids = []
    posts_file = Path(PATH / f"data{INDEX}" / f"{ticker}.csv")
    if posts_file.is_file():
        existing_posts = pd.read_csv( posts_file )
        last_date = existing_posts.iloc[-1]["date"].replace("Z", "")
        end_date = datetime.strptime(last_date, "%Y-%m-%dT%H:%M:%S.%f").date()
        existing_ids = existing_posts["uri"].tolist()
    
    #imposto data iniziale, X (years) prima dell'ultimo post, se presente
    start_date = end_date - timedelta(days=365*years)

    print( f"Scarico post relativi a {ticker}" )
    data = []
    cursor = None
    while True:
        try:
            results = client.app.bsky.feed.search_posts(
                params={
                "q": f'"${ticker}"',    #match esatto
                "since": start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "until": end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "limit":100,
                "lang":"en",
                "cursor":cursor
                }
            )
            posts = results.posts
            if not posts:
                break
            for post in posts:
                post_id = post.uri
                #controllo che il post non sia già stato scaricato
                if post_id not in existing_ids:
                    record = post['record']
                    text = record.text.strip()
                    text = re.sub(r'[\r\n\u2028\u2029]+', ' ', text)

                    #controllo che post contenga il ticker
                    if f"${ticker}".lower() not in text.lower():
                        continue
                    #controllo che il post contenga almeno una delle keys
                    if not any(key in text.lower() for key in keys):
                        continue
                    
                    data.append({
                        'userName': post['author'].handle,
                        'date': standardize_date( record.created_at ),
                        'text': text,
                        'likes': post.like_count,
                        'replies': post.reply_count,
                        'uri': post_id
                    })

            cursor = results.cursor
            if not cursor:
                break

            time.sleep(0.5)  #rispetta i limiti dell'API
        
        except Exception as e:
            if "429" in str(e):
                print("Limite API. Aspetto 60s...")
                time.sleep(60)
                continue
            else:
                print(e)
                time.sleep(5)
    
    print( f"Raccolti {len(data)} post relativi a {ticker}\n" )

    if data:
        new_posts = pd.DataFrame( data )
        if existing_ids:
            df = pd.concat([existing_posts, new_posts ], ignore_index=True )
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values(by="date", ascending=False)
            df.set_index("date", inplace=True)
            df.to_csv( posts_file )
        else:
            new_posts.set_index("date", inplace=True)
            new_posts.to_csv( posts_file )


#login
client = Client()
client.login(USERNAME, PASSWORD)

INDEX_PATH = PATH /".."/"indexes"
INDEX = "MS50"

if __name__ == "__main__":
    start_ticker = 0 
    end_tickers = 10
    keys = ["stock", "stocks", "buy", "sell", "price", "earnings", "analysis", "investment", "hold", "bullish", "bearish", "moon", "pump", "dump", "bagholder", "rocket", "diamond hands", "squeeze"]
    
    df = pd.read_csv( INDEX_PATH / f"{INDEX}.csv" )
    tickers = df["Ticker"].iloc[start_ticker:end_tickers].tolist()
    
    for ticker in tickers:
        get_posts( ticker, keys, 1 ) 