# from amadeus import Client, ResponseError
from credentials import keys
import requests
from datetime import date
import json
from credentials import keys

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

def get_attractions(location, api_key):
    headers = {
        "Accept": "application/json",
        "Authorization": api_key
    }
    url = 'https://api.foursquare.com/v3/places/search'

    for location in neighborhoods:
        data = []
        params = {
            'categories': 16000, # attractions
            'limit': 50,
            'near': f'{location}, Barcelona, Spain'.replace(' ', '+'),
            'fields': fields
        }

        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data += response.json()['results']
            print("getting")
        elif response.status_code == 400:
            print('400 Bad Request')
            break
        print(location)


        to_csv(data, f'/home/bdm/triphawk/data/attractions/{str(location.replace(" ", ""))}_{date.today().strftime("%Y%m%d")}')

get_attractions(neighborhoods, keys.api['foursquare']['api_key'])
