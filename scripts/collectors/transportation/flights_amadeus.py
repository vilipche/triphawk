from amadeus import Client, ResponseError
from credentials import keys

amadeus = Client(
    client_id=keys.api['amadeus']['client_id'],
    client_secret=keys.api['amadeus']['client_secret'],
)

# try:
resp = amadeus.shopping.activities.by_square.get(
    north=41.397158,
    west=2.160873,
    south=41.394582,
    east=2.177181,
    credentials='NIGHTLIFE'
)
# resp = amadeus.get('https://test.api.amadeus.com/v1/reference-data/locations/pois/by-square?north=41.397158&west=2.160873&south=41.394582&east=2.177181&page[limit]=10&page[offset]=0')

print(resp.result)


#     print(response.data)
# except ResponseError as error:
#     print(error)