import requests
import pandas as pd
import time
from io import StringIO
from bs4 import BeautifulSoup
from pathlib import Path

#salvo percorso assoluto del file
PATH = Path(__file__).parent
DATA_PATH = PATH / "data"

#scarico da FINRA shortQuantity e daysToCover mensili per tutti i (tickers), per gli scorsi (years) e salvo in (file_name)
def get_short_interest(tickers, file_name, years):
    #calcolo data prima della quale non dovrò più scaricare
    date_limit = pd.Timestamp.today() - pd.DateOffset(years=years)

    #url per ottenere via AJAX tutti i csv, come accedere da web a https://www.finra.org/finra-data/browse-catalog/equity-short-interest/data
    ajax_url = "https://www.finra.org/views/ajax?_wrapper_format=drupal_ajax&view_name=transparency_services&view_display_id=equity_short_interest_biweekly&custom_month[month]=any&custom_year[year]=any"
    
    #metto header per evitare blocchi, fingo di essere un browser 
    headers = { "User-Agent": "Mozilla/5.0", "Referer": "https://www.finra.org/" }
    
    #faccio la richiesta
    r = requests.get(ajax_url, headers=headers)
    
    #controllo se richiesta va a buon fine
    if r.status_code != 200:
        print( f"Error code: {r.status_code}" )
        return

    #parso per cercare più facilmente i link
    soup = BeautifulSoup(r.json()[3]["data"], "html.parser")
    links = soup.find_all("a", href=True)

    #prendo tutti i link a file csv che trovo
    file_urls = [link["href"] for link in links if link["href"].endswith(".csv")]

    print(f"Trovati {len(file_urls)} file.")

    all_data = []

    #scorro tutti gli url
    for url in file_urls:
        #estraggo data dal nome file
        filename = url.split("/")[-1]
        date_part = "".join(filter(str.isdigit, filename))
        file_date = pd.to_datetime(date_part, format="%Y%m%d")
        
        #controllo per prendere solo x anni
        if file_date <= date_limit:
            continue

        try:
            print(f"Scarico dati per {url}")
            r = requests.get(url, headers=headers)
            if r.status_code != 200:
                print(f"Errore {r.status_code} su {url}")
                continue
            
            #carico il file
            df = pd.read_csv(StringIO(r.text), sep="|", on_bad_lines="skip")

            #prendo solo i ticker contenuti in (tickers)
            df = df[df["symbolCode"].isin(tickers)]

            #se non è vuoto aggiungo ai dati totali
            if not df.empty:
                #formatto data, per evitare problemi dopo
                df["settlementDate"] = file_date
                all_data.append(df)
            
            time.sleep(1)
        except Exception as e:
            print(f"Errore con {url}: {e}")

    #se ci sono dei risultati
    if all_data:
        #unisco tutti ticker
        df_all = pd.concat(all_data)

        #aggiungo colonna per il mese preciso
        df_all["month"] = df_all["settlementDate"].dt.to_period("M").dt.to_timestamp()

        #aggrego per ticker e mese (prendo ultimo valore)
        df_monthly = ( df_all.sort_values("settlementDate").groupby(["symbolCode", "month"]).last().reset_index() )

        # Salvo in csv
        df_monthly.to_csv(DATA_PATH / file_name, index=False)

    else:
        print("Nessun dato trovato per i tickers")


if __name__ == "__main__":
    #carico tutti i tickers
    tickers = pd.read_csv( DATA_PATH / "R2000_2025_tickers.csv", usecols=["Ticker"] ).iloc[:, 0].dropna().unique().tolist()

    get_short_interest( tickers, "R2000_2025_short_interest.csv", 1 )
    
    

    
        