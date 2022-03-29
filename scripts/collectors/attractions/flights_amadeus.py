from amadeus import Client, ResponseError
from credentials import keys

amadeus = Client(
    client_id=keys.api['amadeus']['client_id'],
    client_secret=keys.api['amadeus']['client_secret'],
)

# Latitude North: 41.42, Longitude West: 2.11, Latitude South: 41.347463, Longitude East: 2.228208

resp = amadeus.reference_data.locations.points_of_interest.by_square.get(
    north=41.42,
    west=2.11,
    south=41.347463,
    east=2.228208,
    categories='NIGHTLIFE',
    limit=5,
    page=14
)
print(resp.result)
def get_businesses(location, term, api_key):
    headers = {'Authorization': 'Bearer %s' % keys.api['yelp']['api_key']}
    url = 'https://api.yelp.com/v3/businesses/search'
    data = []
    for offset in range(0, 1000, 50):
        params = {
            'limit': 50,
            'location': location.replace(' ', '+'),
            'term': term.replace(' ', '+'),
            'offset': offset
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data += response.json()['businesses']
            print("getting")
        elif response.status_code == 400:
            print('400 Bad Request')
            break
        print(offset)
    return data
