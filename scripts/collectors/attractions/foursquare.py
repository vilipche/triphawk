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


def fetch_attractions(location, api_key):
    headers = {
        "Accept": "application/json",
        "Authorization": api_key
    }
    url = 'https://api.foursquare.com/v3/places/search'

    current_date = date.today().strftime("%Y%m%d")
    new_dir = f'/user/bdm/triphawk/data/attractions/{current_date}/'

    try:
        loader.create_directory_hdfs(f'{new_dir}')
    except:
        print("Folders already exist")

    for location in neighborhoods:
        params = {
            'categories': 16000, # attractions code
            'limit': 50,
            'near': f'{location}, Barcelona, Spain'.replace(' ', '+'),
            'fields': fields
        }
        response = requests.get(url, headers=headers, params=params)

        try:
            loader.create_directory_hdfs(new_dir)
        except:
            print("ERROR: Directory already exist")

        if response.status_code == 200:
            print(location)
            res = response.json()['results']
            
            loader.add_json_to_hdfs(new_dir, f"{location}.json", {'key': res})

        elif response.status_code == 400:
            print('400 Bad Request')
            break

def get_attractions():
    print("fetching attractions")
    fetch_attractions(neighborhoods, api_key)


get_attractions()