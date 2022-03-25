from credentials import keys
import requests
def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(keys.api['amadeus']['grant_type'])  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    api_url = 'https://test.api.amadeus.com/v1/security/oauth2/token" \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d "grant_type=client_credentials&client_id=oZVuzVomdOzmsvsXHMpsr5ycDknJVEw6&client_secret=MSqoUtMGcAuHI6nw'
    response = requests.post(api_url)
    print(response.json())
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
