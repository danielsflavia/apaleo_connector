import os
import json
import requests
from dotenv import load_dotenv
from auth import get_access_token 

BASE_URL = os.getenv("APALEO_BASE_URL")

def infer_schema_from_sample(data):
   # Recursively infer the types of a nested JSON object.
    if isinstance(data, dict):
        return {k: infer_schema_from_sample(v) for k, v in data.items()}
    elif isinstance(data, list) and data:
        return [infer_schema_from_sample(data[0])]
    else:
        return type(data).__name__

def get_schema(relative_path: str, list_key: str = None):
    """
    Calls an Apaleo API endpoint and infers the schema based on a sample record.
    
    Args:
        relative_path: API path like "/booking/v1/reservations"
        list_key: optional, used to extract the list (e.g. "reservations")

    Returns:
        dict: JSON-style schema
    """
    token = get_access_token()
    url = f"{BASE_URL}{relative_path}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    # If response has a nested list under a key (e.g. {"reservations": [...]})
    if list_key and isinstance(data.get(list_key), list) and data[list_key]:
        return infer_schema_from_sample(data[list_key][0])

    # If response is a direct list (e.g. [...] not wrapped in a key)
    elif isinstance(data, list) and data:
        return infer_schema_from_sample(data[0])

    # Fallback: empty or unexpected format
    return {}
