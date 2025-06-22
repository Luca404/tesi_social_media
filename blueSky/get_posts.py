from atproto import Client
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timedelta
import os
import re
import time

#salvo percorso assoluto del file
PATH = Path(__file__).parent

#carico credenziali
load_dotenv( PATH / "keys.env" )
USERNAME = os.getenv( "USERN" )
PASSWORD = os.getenv( "PASSW" )

#login
client = Client()
client.login(USERNAME, PASSWORD)

INDEX = "SP500"

def get_posts( ticker, name, keys, years ):
    cursor = None
    data = []
    keyword = f'"${ticker}"' #match esatto 
    years_ago = datetime.now() - timedelta(days=365*years)

    print( f"Scarico post relativi a {ticker}" )
    while True:
        try:
            results = client.app.bsky.feed.search_posts(
                params={
                "q":keyword,
                'since': years_ago.strftime("%Y-%m-%dT00:00:00Z"),
                "limit":100,
                "lang":"en",
                "cursor":cursor
                }
            )
            posts = results.posts
            if not posts:
                break

            c = 0
            for post in posts:
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
                    'date': record.created_at,
                    'text': text,
                    'likes': post.like_count,
                    'replies': post.reply_count,
                    'uri': post.uri
                })
                c += 1

            #print( f"Salvati {c} post (su ~100 scaricati)" )

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
        df = pd.DataFrame( data )
        df = df.set_index( "date" )
        df.to_csv( PATH / f"data{INDEX}" / f"{ticker}.csv" )


if __name__ == "__main__":
    start_ticker = 0 
    end_tickers = 10
    keys = ["stock", "stocks", "buy", "sell", "price", "earnings", "analysis", "investment", "hold", "bullish", "bearish", "moon", "pump", "dump", "bagholder", "rocket", "diamond hands", "squeeze"]
    
    df = pd.read_csv( PATH / ".." / f"{INDEX}.csv" )
    tickers = df["Ticker"].iloc[start_ticker:end_tickers].tolist()
    names = df["Name"].iloc[start_ticker:end_tickers].tolist()

    for ticker, name in zip(tickers, names):
        get_posts( ticker, name, keys, 1 )