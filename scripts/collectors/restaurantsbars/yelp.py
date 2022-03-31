import requests
import json
import os
from datetime import date
from credentials import keys

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

def to_csv(data, file_name):
    with open(f'{file_name}.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def fetch_business(terms, neighborhoods, api_key):
    headers = {'Authorization': 'Bearer %s' % api_key}
    url = 'https://api.yelp.com/v3/businesses/search'

    current_date = date.today().strftime("%Y%m%d")
    new_dir = f'/home/bdm/triphawk/data/attractions/{current_date}'
    print(current_date)
    # create a new directory if it doesn't exist 
    # sometimes we have to run it twice in a day
    if(os.path.isdir(new_dir) == False):
        os.mkdir(new_dir)

    for term in terms:
        for location in neighborhoods:
            data = []
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
                    print(f'PRINTING: {offset}_{term}_{location}_{date.today().strftime("%Y%m%d")}')
                elif response.status_code == 400:
                    print('400 Bad Request')
                    break
            
            to_csv(data, f'/home/bdm/triphawk/data/attractions/{term}_{str(location.replace(" ", ""))}_{current_date}')

    return data


def get_businesses():
    print("fetching business")
    fetch_business(terms, neighborhoods, api_key)

get_businesses()