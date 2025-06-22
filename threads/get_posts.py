import requests
import json
from pathlib import Path
from dotenv import load_dotenv
import os

#salvo percorso assoluto del file
PATH = Path(__file__).parent

#carico api key
load_dotenv( PATH / "keys.env" )
ACCESS_TOKEN = os.getenv( "ACCESS_TOKEN" )

BASE_URL = "https://graph.threads.net/v1.0"

def get_posts( keyword, search_type ):
    posts = []
    url = f"{BASE_URL}/keyword_search"
    params = {
        "q": keyword,
        "search_type": search_type,
        "fields": "id,text,timestamp,permalink",
        "access_token": ACCESS_TOKEN
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        posts = response.json().get("data", [])
    except requests.RequestException as e:
        print(f"Errore nella richiesta: {e}")
        print(f"Risposta del server: {response.text}")

    if posts:
        print(f"Trovati {len(posts)} post per la parola chiave '{keyword}':")
        for post in posts:
            print("\nPost ID:", post["id"])
            print("Testo:", post.get("text", "N/A"))
            print("Data:", post.get("timestamp", "N/A"))
            print("Link:", post.get("permalink", "N/A"))
            print("-" * 50)
    else:
        print(f"Nessun post trovato per la parola chiave '{keyword}'. Verifica il token o i permessi.")


if __name__ == "__main__":
    
    #scarica post relativi a (ticker), prende i più "RECENT"/"TOP"
    get_posts( "AAL", "RECENT" )
