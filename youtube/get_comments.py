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


#cerca i video relativi a (ticker), che contengano almeno una (keys), negli scorsi (months)
def search_videos( ticker, keys, months ):
    #carico video già presenti
    video_existing_ids = []
    videos_file = Path(PATH / f"data{INDEX}" / f"{ticker}_titles.csv")
    if videos_file.is_file():
        existing_videos = pd.read_csv( videos_file )
        video_existing_ids = existing_videos["videoId"].tolist()

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
            video_id = video["id"]["videoId"]
            
            #controllo che il video non sia già esistente
            if video_id not in video_existing_ids:
                title = video["snippet"]["title"]
                description = video["snippet"]["description"]
                
                #controllo che il titolo o la descrizione contengano il ticker stesso
                if ticker.lower() not in title.lower() and ticker.lower() not in description.lower():
                    continue

                #controllo che il titolo o la descrizione contengano almeno una delle keys
                if not any(key in title.lower() or key in description.lower() for key in keys):
                    continue

                videos.append({ "videoDate": video["snippet"]["publishedAt"],
                                "videoId": video_id, 
                                "videoTitle": title })
        
        #Scorre a prossima pagina se disponibile
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    
    print( f"Scaricati {len(videos)} video per la keyword {keyword}" )

    if videos:
        videos_df = pd.DataFrame( videos )
        #considero video già presenti
        if video_existing_ids:
            #aggiungo nuovi commenti
            data = pd.concat([existing_videos, videos_df], ignore_index=True)
            #sorto per data per averli in ordine cronologico
            data["videoDate"] = pd.to_datetime(data["videoDate"])
            data = data.sort_values(by="videoDate", ascending=False)
            #salvo in csv
            data.set_index("videoDate", inplace=True)
            data.to_csv( videos_file )
        else:
            videos_df.set_index("videoDate", inplace=True)
            videos_df.to_csv( videos_file )
    
    return videos


#Cerca i primi (limit) commenti da una lista di (videos)
def download_comments_from_videos( ticker, videos, limit ):
    #carico commenti già presenti
    comment_existing_ids = []
    comments_file = Path(PATH / f"data{INDEX}" / f"{ticker}_comments.csv")
    if comments_file.is_file():
        existing_comments = pd.read_csv( comments_file )
        comment_existing_ids = existing_comments["commentId"].tolist()
    
    comments = []
    
    #scorro tutti i video
    for video in videos:
        try:
            response = youtube.commentThreads().list(
                part="snippet",
                videoId=video["videoId"],
                maxResults=limit, #max commenti per video
                textFormat="plainText",
                order="relevance"
            ).execute()
            
            for item in response["items"]:
                comment_id = item["snippet"]["topLevelComment"]["id"]

                #controllo che il commento non sia già presente
                if comment_id not in comment_existing_ids:
                    comment_text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"].strip()
                    comment_text = re.sub(r'[\r\n\u2028\u2029]+', ' ', comment_text)
                    comments.append({
                        "videoDate": video["videoDate"],
                        "videoId": video["videoId"],
                        "videoTitle": video["videoTitle"],
                        "commentDate": item["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                        "commentId": comment_id,
                        "userName": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        "comment_text": comment_text,
                        "likes": item["snippet"]["topLevelComment"]["snippet"]["likeCount"]    
                    })

        #evita problemi per video con commenti disabilitati    
        except:
            pass
    
    print( f"Scaricati {len(comments)} commenti\n" )
    
    if comments:
        comments_df = pd.DataFrame( comments )
        #considero commenti già presenti
        if comment_existing_ids:
            #aggiungo nuovi commenti
            data = pd.concat([existing_comments, comments_df], ignore_index=True)
            #sorto per data per averli in ordine cronologico
            data["commentDate"] = pd.to_datetime(data["commentDate"])
            data = data.sort_values(by="commentDate", ascending=False)
            #salvo in csv
            data.set_index("commentDate", inplace=True)
            data.to_csv( comments_file )
        else:
            comments_df.set_index("commentDate", inplace=True)
            comments_df.to_csv( comments_file )


INDEX_PATH = PATH /".."/"indexes"
INDEX = "MS8"
youtube = build("youtube", "v3", developerKey=API_KEY)

if __name__=="__main__":
    df = pd.read_csv( INDEX_PATH / f"{INDEX}.csv" )
    tickers = df["Ticker"].dropna().tolist()

    #il titolo/descrizione del video deve contenere almeno una di queste
    keys = ["stock", "stocks", "buy", "sell", "price", "earnings", "analysis", "investment", "hold", "bullish", "bearish", "moon", "pump", "dump", "bagholder", "rocket", "diamond hands", "squeeze"]

    for ticker in tickers:
        #cerca video relativi a (ticker), che contengano almeno una (keys), negli scorsi (months)
        videos = search_videos( ticker, keys, 12 )

        #scarica i primi (limit) commenti da lista (videos)
        download_comments_from_videos( ticker, videos, 20 )
        
        
    
    