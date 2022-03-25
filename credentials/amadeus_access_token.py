import requests
from requests.structures import CaseInsensitiveDict
import keys
url = "https://test.api.amadeus.com/v1/security/oauth2/token"

headers = CaseInsensitiveDict()
headers["Content-Type"] = "application/x-www-form-urlencoded"

data = f"grant_type=client_credentials&client_id={keys.api['amadeus']['client_id']}&client_secret={keys.api['amadeus']['client_secret']}"

try:
    resp = requests.post(url, headers=headers, data=data)
    keys.api['amadeus']['access_token'] = resp.json()['access_token']

finally:
    print(resp.json())
    print("access_token:", keys.api['amadeus']['access_token'])
