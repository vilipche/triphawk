import requests
import json
import os
from datetime import date
from credentials import keys
from scripts.loaders import loader

api_key = keys.api['foursquare']['api_key']

fields='fsq_id,name,geocodes,location,categories,chains,related_places,timezone,distance,description,tel,email,website,social_media,hours,rating,popularity,price,date_closed'
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


def to_csv(data, file_name):
    with open(f'{file_name}.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def fetch_attractions(location, api_key):
    headers = {
        "Accept": "application/json",
        "Authorization": api_key
    }
    url = 'https://api.foursquare.com/v3/places/search'

    current_date = date.today().strftime("%Y%m%d")
    new_dir = f'/home/bdm/triphawk/data/attractions/{current_date}'

    try:
        os.makedirs(f'{new_dir}')
    except FileExistsError:
        print("Folders already exist")

    for location in neighborhoods:
        data = []
        params = {
            'categories': 16000, # attractions code
            'limit': 50,
            'near': f'{location}, Barcelona, Spain'.replace(' ', '+'),
            'fields': fields
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data += response.json()['results']
            print(type(response.json()['results']))
            print(type(data))
            return
            print("getting")
        elif response.status_code == 400:
            print('400 Bad Request')
            break
        print(location)


        to_csv(data, f'{new_dir}/{str(location.replace(" ", ""))}_{current_date}')

def get_attractions():
    print("fetching attractions")
    fetch_attractions(neighborhoods, api_key)


get_attractions()