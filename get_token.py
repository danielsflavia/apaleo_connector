import requests

def get_access_token():
    url = "https://identity.apaleo.com/connect/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": "CYME-SP-CONNECTOR",
        "client_secret": "f7dSHAYgpftgG9BYq3h4Z7n5Mtie7r",
        "scope": "reservations.read folios.read setup.read"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url, data=data, headers=headers)
    response.raise_for_status()
    return response.json()["access_token"]

if __name__ == "__main__":
    token = get_access_token()
    print("Access Token:", token)
