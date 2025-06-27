import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path

DATA_PATH = Path(__file__).parent / "data"

def get_squeeze(urls, outputName):
    data = []
    for url in urls:
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")
        soup.find_all('a', href=True)
        links = [a for a in soup.find_all("a", href=True) if "/short-squeeze/" in a["href"] and "2" in a["href"] and not "#respond" in a["href"] ]
        
        for link in links:
            if link.has_attr("class"):
                title = link.get("title")
                try:
                    ticker = title.split(":")[1].split(")")[0]
                    date = title.split( " " )[-1]
                    data.append({"date":date, "ticker":ticker})
                except:
                    pass
        
    df = pd.DataFrame(data)
    df = df.sort_values(["ticker", "date"])
    df.to_csv( DATA_PATH/f"{outputName}.csv", index=False )    


if __name__ == "__main__":
    url = "https://news.squeezereport.com/category/short-squeeze/"
    url1 = "https://news.squeezereport.com/category/short-squeeze/page/2"
    get_squeeze( [url, url1], "historical_squeeze" )
