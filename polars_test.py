import os
import json
import polars as pl
import requests
from dotenv import load_dotenv
from auth import get_access_token
from schema_utils import get_schema

load_dotenv()
BASE_URL = os.getenv("APALEO_BASE_URL")

# Converts nested structures (dicts/lists) to strings
def clean_rows(rows: list[dict]) -> list[dict]:
    return [
        {
            key: json.dumps(value) if isinstance(value, (dict, list)) else value
            for key, value in row.items()
        }
        for row in rows
    ]

# Maps Schema to Polars
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

# Fetches raw Apaleo Data using access token
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

# Example DataFrame loaders
def load_reservations_df():
    rows = fetch_data("/booking/v1/reservations", list_key="reservations")
    schema = get_schema("/booking/v1/reservations", list_key="reservations")["reservations"][0]
    return pl.from_dicts(clean_rows(rows), schema=build_polars_schema(schema))

def load_bookings_df():
    rows = fetch_data("/booking/v1/bookings", list_key="bookings")
    schema = get_schema("/booking/v1/bookings", list_key="bookings")["bookings"][0]
    return pl.from_dicts(clean_rows(rows), schema=build_polars_schema(schema))

def load_folios_df():
    rows = fetch_data("/finance/v1/folios", list_key="folios")
    schema = get_schema("/finance/v1/folios", list_key="folios")["folios"][0]
    return pl.from_dicts(clean_rows(rows), schema=build_polars_schema(schema))

def load_properties_df():
    rows = fetch_data("/inventory/v1/properties", list_key="properties")
    schema = get_schema("/inventory/v1/properties", list_key="properties")["properties"][0]
    return pl.from_dicts(clean_rows(rows), schema=build_polars_schema(schema))

def load_unit_groups_df():
    rows = fetch_data("/inventory/v1/unit-groups", list_key="unitGroups")
    schema = get_schema("/inventory/v1/unit-groups", list_key="unitGroups")["unitGroups"][0]
    return pl.from_dicts(clean_rows(rows), schema=build_polars_schema(schema))

def load_units_df():
    rows = fetch_data("/inventory/v1/units", list_key="units")
    schema = get_schema("/inventory/v1/units", list_key="units")["units"][0]
    return pl.from_dicts(clean_rows(rows), schema=build_polars_schema(schema))

def load_services_df():
    rows = fetch_data("/rateplan/v1/services", list_key="services")
    schema = get_schema("/rateplan/v1/services", list_key="services")["services"][0]
    return pl.from_dicts(clean_rows(rows), schema=build_polars_schema(schema))

def load_capturepolicies_df():
    rows = fetch_data("/settings/v1/capture-policies", list_key="capturePolicies")
    schema = get_schema("/settings/v1/capture-policies", list_key="capturePolicies")["capturePolicies"][0]
    return pl.from_dicts(clean_rows(rows), schema=build_polars_schema(schema))

# Example usage
if __name__ == "__main__":

    print("\n--- Reservations Table ---")
    df_reservations = load_reservations_df()

    print("\n--- Unit Groups Table ---")    
    df_unit_groups = load_unit_groups_df()

    print("\n--- Units Table ---")
    df_units = load_units_df()

    '''
    print("\n--- Bookings Table ---")
    df_booking = load_bookings_df()
    print(df_booking.head())

    print("\n--- Folios Table ---")
    df_folios = load_folios_df()
    print(df_folios.head())

    print("\n--- Properties Table ---")
    df_properties = load_properties_df()
    print(df_properties)

    print("\n--- capturePolicies Table ---")
    df_capturepolicies = load_capturepolicies_df()
    print(df_capturepolicies)

    print("\n--- Services Table ---")
    df_services = load_services_df()
    print(df_services)

    # Filter all reservations from hotel 'VIE'
    df_reservations_vie = df_reservations.filter(pl.col("property").str.contains('"id": "VIE"'))
    print("\nReservations in Hotel VIE:")
    print(df_reservations_vie)


    # Decode balance JSON string into struct
    df_folios = df_folios.with_columns([
        pl.col("balance").str.json_decode().alias("balance_parsed")
    ])

    # Extract 'amount' and 'currency' as new columns
    df_folios = df_folios.with_columns([
        pl.col("balance_parsed").struct.field("amount").alias("amount"),
        pl.col("balance_parsed").struct.field("currency").alias("currency")
    ])

    # Define conversion rates
    conversion_rates = {
        "EUR": 1.0,
        "GBP": 1.15,
        "USD": 0.85
    }

    # Add conversion rate and converted EUR amount
    df_folios = df_folios.with_columns([
        pl.col("currency").map_elements(
            lambda c: conversion_rates.get(c, 1.0),
            return_dtype=pl.Float64
        ).alias("conversion_rate"),
        (pl.col("amount") * pl.col("currency").map_elements(
            lambda c: conversion_rates.get(c, 1.0),
            return_dtype=pl.Float64
        )).alias("amount_eur")
    ])

    # Sort by amount and amount in EUR
    df_sorted = df_folios.sort(["amount", "amount_eur"])
    print("\nSorted & Converted Folio Balances to EUR :")
    print(df_sorted)

  '''
    df_reservations = df_reservations.with_columns([
        pl.col("property").str.json_decode().alias("property_parsed")
    ])
    df_reservations = df_reservations.with_columns([
        pl.col("property_parsed").struct.field("id").alias("property_id")
    ])
  
    # Group by property_id and count occurrences
    df_counts = df_reservations.group_by("property_id").agg([
        pl.len().alias("count")
    ]).sort("count", descending=True)
    print("\n--- Number of Reservations per Hotel ---")
    print(df_counts)

    # Unit Groups types per hotel
    df_unit_groups = df_unit_groups.with_columns([
        pl.col("property").str.json_decode().alias("property_parsed")
    ])
    df_unit_groups = df_unit_groups.with_columns([
        pl.col("property_parsed").struct.field("id").alias("property_id")
    ])

    df_unit_breakdown = df_unit_groups.select([
        pl.col("property_id"),
        pl.col("code"),          
        pl.col("name"),           
        pl.col("memberCount")    
    ])

    df_unit_breakdown = df_unit_breakdown.sort(["property_id", "code"])
    print("\n--- Unit Types and Room Counts per Property ---")
    print(df_unit_breakdown)
 

    # Reservations per Hotel
    print("Reservations per Hotel")
    df_counts = df_reservations.group_by("property_id").agg([
        pl.len().alias("reservations")
    ]).sort("reservations", descending=True)
    print(df_counts)

    # Reservations per Hotel Room units
    print("Number of Units and Total Guest Capacity per Property and Room Type")
    df_units = df_units.with_columns([
        pl.col("property").map_elements(lambda s: json.loads(s).get("id"), return_dtype=pl.String).alias("property_id"),
        pl.col("unitGroup").map_elements(lambda s: json.loads(s).get("id"), return_dtype=pl.String).alias("unit_group_id"),
        pl.col("status").map_elements(lambda s: json.loads(s).get("isOccupied"), return_dtype=pl.Boolean).alias("is_occupied"),
    ])


    df_unit_summary = df_units.group_by(["property_id", "unit_group_id"]).agg([
        pl.len().alias("num_units"),
        pl.sum("maxPersons").alias("total_capacity"),
        pl.sum("is_occupied").alias("units_occupied"),
    ])

    print(df_unit_summary)