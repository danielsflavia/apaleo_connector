import os
import requests
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
    token = get_access_token()
    url = f"{BASE_URL}{relative_path}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    schema = {}

    # Add top-level metadata like 'count'
    if isinstance(data, dict):
        for key, value in data.items():
            # If it's the list_key, we handle it differently
            if key == list_key and isinstance(value, list) and value:
                schema[key] = [infer_schema_from_sample(value[0])]
            elif key != list_key:
                schema[key] = type(value).__name__

    # If response is a direct list (not wrapped)
    elif isinstance(data, list) and data:
        schema = infer_schema_from_sample(data[0])

    return schema
