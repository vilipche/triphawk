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
    east=2.177181
)

print(resp.data)

#     print(response.data)
# except ResponseError as error:
#     print(error)