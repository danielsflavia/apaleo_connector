# Apaleo Connector

A modular HTTP interface for accessing structured transactional data from the Apaleo API. This connector enables seamless integration between Apaleo and data processing tools (e.g. Polars, Dashboards, Analytics engines) by exposing key endpoints over a local server.

## Overview

This connector acts as a lightweight bridge to Apaleo’s API-first PMS platform. It facilitates access to key hotel data—reservations, financials, and property metadata—through authenticated, stateless GET requests.

**Core features:**

- Modular architecture using Python’s built-in HTTP server
- OAuth 2.0 client credentials flow abstracted via `auth.py`
- Configurable via environment variables in `.env`
- Easy to extend with additional endpoints or data transformations
- Returns raw Apaleo JSON for downstream processing

## Environment Variables

The connector reads configuration from a `.env` file:
```bash
APALEO_CLIENT_ID=
APALEO_CLIENT_SECRET=
APALEO_SCOPES=reservations.read folios.read setup.read
APALEO_BASE_URL=https://api.apaleo.com
```

## Implemented Endpoints

The following GET routes are exposed locally and map directly to Apaleo API endpoints:

### `/reservations`

- **Upstream**: `GET /booking/v1/reservations`
- **Returns**: Reservation data including booking ID, guest info, arrival/departure, and channel code.

### `/bookings`

 - **Upstream**: `GET /booking/v1/bookings`
- **Returns**: Individual room data with ID, name, description, and linked unit group.

### `/folios`

- **Upstream**: `GET /finance/v1/folios`
- **Returns**: Folio information such as charges, payments, balances — useful for financial reporting.

### `/properties`

- **Upstream**: `GET /inventory/v1/properties`
- **Returns**: Property metadata including IDs, names, and descriptions.

### `/unit-groups`

- **Upstream**: `GET /inventory/v1/unit-groups`
- **Returns**: Room category data (e.g. Single, Double) including ID, name, and capacity.

### `/units`

- **Upstream**: `GET /inventory/v1/units`
- **Returns**: Individual room data with ID, name, description, and linked unit group.

### `/sources`

- **Upstream**: `GET /booking/v1/types/sources`
- **Returns**:  A list of supported booking sources (e.g. Direct, Expedia, Booking.com) used to filter or analyze reservation channels.

### `/services`

- **Upstream**: `GET /rateplan/v1/services`
- **Returns**:  A list of services each Hotel offers (e.g. Breakfast, Wifi, Yoga).

### `/capture-policies`

- **Upstream**: `GET /settings/v1/capture-policies`
- **Returns**:  A list of capture policies (e.g. CancellationFee, Prepayment).

## `


For full access to all available endpoints and details on request parameters, visit the official Apaleo Swagger documentation:
[https://api.apaleo.com/swagger/index.html](https://api.apaleo.com/swagger/index.html)

---

## Usage Notes

- The server listens on port `8000` and serves JSON from each route.
- OAuth tokens are automatically fetched via the `auth.py` module on each request.
- To add more endpoints, extend the `ApaleoHandler.do_GET()` method by adding an `elif path == "/your-endpoint"` block and calling `fetch_data_from_apaleo()` with the respective API path.
- Error responses from Apaleo (e.g. 401, 403, 404) are passed through as 500 status codes with the message included in the response body.
- This implementation is suitable for sandboxing, prototyping, or integration testing — not intended for production deployments without security, rate limiting, and logging enhancements.
