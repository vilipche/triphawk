import requests
import json
import os
from datetime import date
from credentials import keys
from scripts.loaders import loader

api_key = keys.api['yelp']['api_key']

neighborhoods = [    'Barceloneta',
    'Barri Gòtic',
    'Diagonal Mar i Forum',
    'El Poble-sec',
    'El Raval',
    'Gràcia',
    'Horta-Guinardó',
    "L'Eixample",
    "La Ciudatella",
    "La Vila Olímpica",
    "Les Corts",
    'Montjuïc',
    "Nou Barris",
    "Parc i Llacuna del Poblenou",
    "Poblenou",
    "Sant Andreu",
    "Sant Martí",
    "Sant Pere, Santa Caterina i la Ribera-Born",
    "Sants",
    "Sarrià - Sant Gervasi"]
terms = ['bar', 'restaurant']


def fetch_business(terms, neighborhoods, api_key):
    headers = {'Authorization': 'Bearer %s' % api_key}
    url = 'https://api.yelp.com/v3/businesses/search'

    current_date = date.today().strftime("%Y%m%d")
    new_dir = f'/user/bdm/triphawk/data/businesses/{current_date}'

    try:
        loader.create_directory_hdfs(f'{new_dir}')
    except:
        print("Folders already exist")


    for location in neighborhoods:
        for term in terms:
            data = []
            print(f'{term}_{location}')
            for offset in range(0, 1000, 50):
                params = {
                    'limit': 50,
                    'location': f'{location}, Barcelona, Spain'.replace(' ', '+'),
                    'term': term.replace(' ', '+'),
                    'offset': offset
                }

                response = requests.get(url, headers=headers, params=params)

                if response.status_code == 200:
                    data += response.json()['businesses']
                elif response.status_code == 400:
                    print('400 Bad Request')
                    break

            loader.add_json_to_hdfs(f'{new_dir}/{term}/', f"{term}_{location}_{current_date}.json", {'key': data})

    return data

def get_businesses():
    print("fetching business")
    fetch_business(terms, neighborhoods, api_key)

get_businesses()