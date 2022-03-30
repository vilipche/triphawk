import requests
import json
from datetime import date
from credentials import keys

api_key = 'copy_your_yelp_api_key_here'

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

def get_businesses(terms, neighborhoods):
    headers = {'Authorization': 'Bearer %s' % keys.api['yelp']['api_key']}
    url = 'https://api.yelp.com/v3/businesses/search'

    for term in terms:
        for location in neighborhoods:
            data = []
            for offset in range(0, 100, 50):
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

            to_csv(data, f'../../../data/restaurantsbars/{term}_{str(location.replace(" ", ""))}_{date.today().strftime("%Y%m%d")}')
    return data


def run_this():
    get_businesses(terms, neighborhoods[:2])
    print("done")

run_this()