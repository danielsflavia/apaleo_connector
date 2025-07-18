import os
import json
import polars as pl
import requests
from dotenv import load_dotenv
from auth import get_access_token
from schema_utils import get_schema

load_dotenv()
BASE_URL = os.getenv("APALEO_BASE_URL")

def clean_rows(rows: list[dict]) -> list[dict]:
    return [
        {
            key: json.dumps(value) if isinstance(value, (dict, list)) else value
            for key, value in row.items()
        }
        for row in rows
    ]

def build_polars_schema(schema: dict) -> dict:
    def resolve_type(t):
        if isinstance(t, list):
            return pl.List(pl.String)
        elif isinstance(t, dict):
            return pl.String
        elif t == "str":
            return pl.String
        elif t == "int":
            return pl.Int64
        elif t == "float":
            return pl.Float64
        elif t == "bool":
            return pl.Boolean
        else:
            return pl.String
    return {key: resolve_type(t) for key, t in schema.items()}

def fetch_data(relative_path: str, list_key: str = None) -> list[dict]:
    token = get_access_token()
    url = f"{BASE_URL}{relative_path}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    if list_key:
        return data.get(list_key, [])
    elif isinstance(data, list):
        return data
    return []

# Example DataFrame loaders — call only when needed
def load_reservations_df():
    rows = fetch_data("/booking/v1/reservations", list_key="reservations")
    schema = get_schema("/booking/v1/reservations", list_key="reservations")
    return pl.from_dicts(clean_rows(rows), schema=build_polars_schema(schema))

def load_folios_df():
    rows = fetch_data("/finance/v1/folios", list_key="folios")
    schema = get_schema("/finance/v1/folios", list_key="folios")
    return pl.from_dicts(clean_rows(rows), schema=build_polars_schema(schema))

def load_properties_df():
    rows = fetch_data("/inventory/v1/properties", list_key="properties")
    schema = get_schema("/inventory/v1/properties", list_key="properties")
    return pl.from_dicts(clean_rows(rows), schema=build_polars_schema(schema))

def load_unit_groups_df():
    rows = fetch_data("/inventory/v1/unit-groups", list_key="unitGroups")
    schema = get_schema("/inventory/v1/unit-groups", list_key="unitGroups")
    return pl.from_dicts(clean_rows(rows), schema=build_polars_schema(schema))

def load_units_df():
    rows = fetch_data("/inventory/v1/units", list_key="units")
    schema = get_schema("/inventory/v1/units", list_key="units")
    return pl.from_dicts(clean_rows(rows), schema=build_polars_schema(schema))


# Example usage
if __name__ == "__main__":
    print("\n--- Reservations Table ---")
    df_reservations = load_reservations_df()
    print(df_reservations.head())

    # Filter all reservations from hotel 'VIE'
    df_reservations_vie = df_reservations.filter(pl.col("property").str.contains('"id": "VIE"'))
    print("\nReservations in Hotel VIE:")
    print(df_reservations_vie)