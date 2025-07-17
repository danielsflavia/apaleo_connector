import os
import requests
import polars as pl
from dotenv import load_dotenv
from auth import get_access_token  # Import from your auth module

# Load environment variables
load_dotenv()
BASE_URL = os.getenv("APALEO_BASE_URL")

def fetch_reservations(token):
    url = f"{BASE_URL}/booking/v1/reservations"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()["reservations"]

def filter_with_polars(reservations):
    # Flatten selected fields for DataFrame
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

    # Example filter: adults > 1
    filtered = df.filter(pl.col("adults") > 1)

    print("Filtered DataFrame:")
    print(filtered)

if __name__ == "__main__":
    token = get_access_token()
    reservations = fetch_reservations(token)
    filter_with_polars(reservations)

