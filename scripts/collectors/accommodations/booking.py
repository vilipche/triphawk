from distutils.log import info
import requests
from bs4 import BeautifulSoup as bs
import json
import time
import pandas as pd
import re

#TODO: add function to get the availability info

def get_hotel_data(hotel_url):
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
    info_search = re.findall(r'application/ld\+json.*?>(.*?)</script>', response.text, flags= re.S)
    if len(info_search) < 1:
        return None
    data = json.loads(info_search[0])
    return data

def fetch_accommodations(current_date, sleep_time=3):
    url_list = []
    with open("bcn_hotel_url_list.txt", 'r') as f:
        url_list = f.readlines()
    url_list = [url.strip() for url in set(url_list)]
    total = len(url_list)
    print("total accommodation:", total)
    count = 1
    fail_count = 0

    json_list = []
    for hotel_url in url_list:
        time.sleep(sleep_time)
        print("%d/%d"%(count, total), hotel_url)
        data = get_hotel_data(hotel_url)
        if data == None:
            print("[warning] fail to fetch %s" % hotel_url)
            fail_count += 1
            continue
        json_list.append({'data': data, 'date_fetched': current_date, 'hotel_url': hotel_url})
        count += 1
    print("success:", len(json_list), "fail:", fail_count)
    return json_list

def get_accommodations(current_date):
    print("fetching accommodations")
    return fetch_accommodations(current_date)