import requests
from bs4 import BeautifulSoup as bs
import json
import time
import pandas as pd

def get_hotel_info(hotel_url):
    headers ={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36",
    }
    try:
        response = requests.get(hotel_url, headers=headers, timeout=3)
    except Exception as err:
        print(hotel_url, "Exception: %s"%err)
        return None
    if response.status_code != 200:
        return None
    soup = bs(response.text,"lxml")
    try:
        hotel_name = soup.find_all('h2', attrs={"class":"hp__hotel-name"})[0].text.strip()
        if len(hotel_name.split('\n')) >= 2:
            hotel_badge = hotel_name.split('\n')[0]
            hotel_name = hotel_name.split('\n')[1]
        hotel_loc = soup.find_all('span', attrs={"data-node_tt_id":"location_score_tooltip"})[0].text.strip()
        hotel_desc = soup.find_all('div', attrs={"id":"property_description_content"})[0].text.strip()
    except Exception as err:
        print(hotel_url, "Exception: %s"%err)
        return None
    else:
        return {"hotel_name": hotel_name,
                "hotel_loc": hotel_loc,
                "hotel_desc": hotel_desc,
                "hotel_url": hotel_url}

url_list = []
with open("bcn_hotel_url_list.txt", 'r') as f:
    url_list = f.readlines()
url_list = [url.strip() for url in set(url_list)]
total = len(url_list)
count = 1

columns = ["hotel_name", "hotel_loc", "hotel_desc", "hotel_url", ]
df = pd.DataFrame(columns=columns)

for hotel_url in url_list:
    time.sleep(3)
    print("%d/%d"%(count, total), hotel_url)
    res = get_hotel_info(hotel_url)
    if res == None:
        print("[warning] fail to fetch %s" % hotel_url)
        continue
    df = df.append(res, ignore_index=True)
    count += 1

df.to_csv("booking_hotel.csv", sep="|")