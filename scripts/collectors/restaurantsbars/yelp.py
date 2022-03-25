import requests
import json
from credentials import keys

api_key = 'copy_your_yelp_api_key_here'


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

get_businesses('Gracia Barcelona', 'bar',api_key)