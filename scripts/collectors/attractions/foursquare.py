# from amadeus import Client, ResponseError
from credentials import keys
import requests
from datetime import date
import json
from credentials import keys
# amadeus = Client(
#     client_id=keys.api['amadeus']['client_id'],
#     client_secret=keys.api['amadeus']['client_secret'],
# )
# Latitude North: 41.42, Longitude West: 2.11, Latitude South: 41.347463, Longitude East: 2.228208
# amadeus only has one page
# resp = amadeus.reference_data.locations.points_of_interest.by_square.get(
#     north=41.42,
#     west=2.11,
#     south=41.347463,
#     east=2.228208,
#     categories='NIGHTLIFE',
#     limit=5,
#     page=14
# )
# print(resp.result)

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

# url = "https://api.foursquare.com/v3/places/search?categories=16000&fields=fsq_id%2Cname%2Cgeocodes%2Clocation%2Ccategories%2Cchains%2Crelated_places%2Ctimezone%2Cdistance%2Cdescription%2Ctel%2Cemail%2Cwebsite%2Csocial_media%2Chours%2Crating%2Cpopularity%2Cprice%2Cdate_closed&near=Barcelona%2C%20Spain&limit=50"



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


        to_csv(data, f'../../../data/attractions/{str(location.replace(" ", ""))}_{date.today().strftime("%Y%m%d")}')

get_attractions(neighborhoods, keys.api['foursquare']['api_key'])