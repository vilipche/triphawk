import requests
from datetime import date
from credentials import keys
from scripts.temp_loaders import loader

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

def fetch_business(terms, neighborhoods, api_key, current_date):
    headers = {'Authorization': 'Bearer %s' % api_key}
    url = 'https://api.yelp.com/v3/businesses/search'

    data = []
    for location in neighborhoods:
        for term in terms:
            term_data = []

            for offset in range(0, 100, 50):
                params = {
                    'limit': 50,
                    'location': f'{location}, Barcelona, Spain'.replace(' ', '+'),
                    'term': term.replace(' ', '+'),
                    'offset': offset
                }

                response = requests.get(url, headers=headers, params=params)

                if response.status_code == 200:
                    term_data += response.json()['businesses']

                elif response.status_code == 400:
                    print('400 Bad Request')
                    break

            data.append({'data': term_data, 'date_fetched': current_date, 'location': location, 'type': term})

    return data

def get_businesses(current_date):
    print("fetching business")
    return fetch_business(terms, ['Barceloneta'], api_key, current_date)