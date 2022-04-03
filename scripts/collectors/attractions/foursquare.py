import requests
from credentials import keys
from scripts.temp_loaders import loader

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


def fetch_attractions(location, api_key, current_date):
    """
    function that fetches and returns attractions data from API 
    :return: list of objects
    """

    headers = {
        "Accept": "application/json",
        "Authorization": api_key
    }
    url = 'https://api.foursquare.com/v3/places/search'

    data = []
    for location in neighborhoods:
        params = {
            'categories': 16000, # attractions code for forsquare API
            'limit': 50,
            'near': f'{location}, Barcelona, Spain'.replace(' ', '+'),
            'fields': fields
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            # print(location)
            res = response.json()['results']

            data.append({'data': res, 'date_fetched': current_date, 'location': location})

        elif response.status_code == 400:
            print('400 Bad Request')
            break

    return data

def get_attractions(current_date):
    print("fetching attractions")
    return fetch_attractions('Barceloneta', api_key, current_date)