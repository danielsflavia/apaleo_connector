import os
import requests
from dotenv import load_dotenv

load_dotenv()

def get_access_token():
    url = "https://identity.apaleo.com/connect/token"
     # Prepare the data payload for the token request
    data = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("APALEO_CLIENT_ID"),
        "client_secret": os.getenv("APALEO_CLIENT_SECRET"),
        "scope": os.getenv("APALEO_SCOPES")
    }
    # Set headers for the HTTP request
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    # Make a POST request to get the access token
    response = requests.post(url, data=data, headers=headers)
    response.raise_for_status()
    # Return the access token from the response
    return response.json()["access_token"]

if __name__ == "__main__":
    token = get_access_token()
    print("Access Token:", token)
