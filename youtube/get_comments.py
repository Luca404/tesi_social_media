from googleapiclient.discovery import build
from datetime import datetime, timedelta
import pandas as pd
import re
from dotenv import load_dotenv
from pathlib import Path
import time
import os

#salvo percorso assoluto del file
PATH = Path(__file__).parent

#carico api key
load_dotenv( PATH / "keys.env" )
API_KEY = os.getenv( "API_KEY" )

INDEX = "MS50"
youtube = build("youtube", "v3", developerKey=API_KEY)


#cerca i (max) video più recenti riguardo la (keyword)
def search_videos( ticker, keys, months ):
    videos = []
    keyword = f"${ticker}"
    next_page_token = None
    months_ago = (datetime.now() - timedelta(days=months*30)).strftime("%Y-%m-%dT00:00:00Z")
    while True:
        response = youtube.search().list(
            part="snippet",
            q=keyword,
            type="video",
            #order="date", #video più recenti
            maxResults=50,
            publishedAfter=months_ago,  #solo ultimi X mesi
            pageToken=next_page_token
        ).execute()

        for video in response["items"]:
            title = video["snippet"]["title"]
            description = video["snippet"]["description"]
            
            #controllo che il titolo o la descrizione contengano il ticker stesso
            if ticker.lower() not in title.lower() and ticker.lower() not in description.lower():
                continue

            #controllo che il titolo o la descrizione contengano almeno una delle keys
            if not any(key in title.lower() or key in description.lower() for key in keys):
                continue

            videos.append({ "id": video["id"]["videoId"],
                            "date": video["snippet"]["publishedAt"], 
                            "title": title })
        
        #Scorre a prossima pagina se disponibile
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    
    print( f"Scaricati {len(videos)} video per la keyword {keyword}" )
    return videos


#Scarico i primi (max) commenti da una lista di (videos)
def download_comments_from_videos( ticker, videos ):
    titles = []
    comments = []
    
    #scorro tutti i video
    for video in videos:
        #salvo i titoli dei video
        titles.append({"videoDate": video["date"], "videoId": video["id"], "videoTitle": video["title"]})

        try:
            response = youtube.commentThreads().list(
                part="snippet",
                videoId=video["id"],
                maxResults=50, #max commenti per video
                textFormat="plainText",
                order="relevance"
            ).execute()
            
            for item in response["items"]:
                comment_text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"].strip()
                comment_text = re.sub(r'[\r\n\u2028\u2029]+', ' ', comment_text)
                comments.append({
                    "videoDate": video["date"],
                    "videoId": video["id"],
                    "videoTitle": video["title"],
                    "commentDate": item["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                    "commentId": item["snippet"]["topLevelComment"]["id"],
                    "userName": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    "comment_text": comment_text,
                    "likes": item["snippet"]["topLevelComment"]["snippet"]["likeCount"]    
                })

        #evita problemi per video con commenti disabilitati    
        except:
            pass
    
    print( f"Scaricati {len(comments)} commenti\n" )

    save_data_to_csv( titles, "videoDate", f"{ticker}_titles" )
    save_data_to_csv( comments, "commentDate", f"{ticker}_comments" )


#Salvo list to .csv 
def save_data_to_csv( data, index, file_name ):
    try:
        df = pd.DataFrame( data )
        df = df.set_index( index )
        df.to_csv( PATH / f"data{INDEX}" / f"{file_name}.csv" )
    
    except Exception as e:
        print( f"{e}\n" )



if __name__=="__main__":
    df = pd.read_csv( PATH / ".." / f"{INDEX}.csv" )
    tickers = df["Ticker"].dropna().iloc[:5].tolist()
    #tickers = ["AAPL"]

    #il titolo del video deve contenere almeno una di queste
    keys = ["stock", "stocks", "buy", "sell", "price", "earnings", "analysis", "investment", "hold", "bullish", "bearish", "moon", "pump", "dump", "bagholder", "rocket", "diamond hands", "squeeze"]

    for ticker in tickers:
        months_ago = 12
        videos = search_videos( ticker, keys, months_ago )

        download_comments_from_videos( ticker, videos )
        
        
    
    