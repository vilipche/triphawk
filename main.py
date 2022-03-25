from credentials import keys
import requests



def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(keys.api['amadeus']['grant_type'])  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    api_url = 'https://test.api.amadeus.com/v2/shopping/flight-offers?originLocationCode=CDG&destinationLocationCode=BCN&departureDate=2021-11-01&adults=1&nonStop=false&max=250'
    response = requests.get(api_url)
    print(response.json())
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
