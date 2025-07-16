import os
import requests
import polars as pl
from dotenv import load_dotenv

# Load .env credentials
load_dotenv()
CLIENT_ID = os.getenv("APALEO_CLIENT_ID")
CLIENT_SECRET = os.getenv("APALEO_CLIENT_SECRET")
SCOPES = os.getenv("APALEO_SCOPES")
BASE_URL = os.getenv("APALEO_BASE_URL")

def get_access_token():
    url = "https://identity.apaleo.com/connect/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPES,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url, data=data, headers=headers)
    response.raise_for_status()
    return response.json()["access_token"]

def fetch_reservations(token):
    url = f"{BASE_URL}/booking/v1/reservations"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()["reservations"]

def filter_with_polars(reservations):
    # Flatten relevant fields
    rows = []
    for res in reservations:
        rows.append({
            "id": res.get("id"),
            "status": res.get("status"),
            "arrival": res.get("arrival"),
            "departure": res.get("departure"),
            "created": res.get("created"),
            "channelCode": res.get("channelCode"),
            "adults": res.get("adults"),
            "totalGross": res.get("totalGrossAmount", {}).get("amount"),
            "currency": res.get("totalGrossAmount", {}).get("currency"),
        })

    df = pl.DataFrame(rows)

    # Beispiel: alle Reservierungen mit mehr als 1 Erwachsenem
    filtered = df.filter(pl.col("adults") > 1)

    print("Filtered Data:")
    print(filtered)

if __name__ == "__main__":
    token = get_access_token()
    reservations = fetch_reservations(token)
    filter_with_polars(reservations)

