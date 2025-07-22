import os
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse
import requests
from dotenv import load_dotenv
from auth import get_access_token  # IMPORT FROM auth.py
from schema_utils import get_schema

# Load BASE_URL from .env
load_dotenv()
BASE_URL = os.getenv("APALEO_BASE_URL")

def fetch_data_from_apaleo(endpoint, token):
    url = f"{BASE_URL}{endpoint}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text

# HTTP request handler class
class ApaleoHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path

        if path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            html = """
            <html>
                <head>
                    <meta charset="UTF-8">
                    <title>Apaleo Connector</title>
                </head>
                <body>
                    <h1>Apaleo Connector</h1>
                    <ul>
                        <li><a href="/reservations">Reservations</a> — <a href="/reservations/schema">Schema</a></li>
                        <li><a href="/bookings">Bookings</a> — <a href="/bookings/schema">Schema</a></li>
                        <li><a href="/folios">Folios</a> — <a href="/folios/schema">Schema</a></li>
                        <li><a href="/properties">Properties</a> — <a href="/properties/schema">Schema</a></li>
                        <li><a href="/unit-groups">Unit Groups</a> — <a href="/unit-groups/schema">Schema</a></li>
                        <li><a href="/units">Units</a> — <a href="/units/schema">Schema</a></li>
                        <li><a href="/sources">Sources</a> — <a href="/sources/schema">Schema</a></li>
                        <li><a href="/services">Services</a> — <a href="/services/schema">Schema</a></li>
                        <li><a href="/capture-policies">capture-policies</a> — <a href="/capture-policies/schema">Schema</a></li>
                    </ul>
                </body>
            </html>
            """
            self.wfile.write(html.encode("utf-8"))

        elif path == "/reservations":
            try:
                token = get_access_token()
                data = fetch_data_from_apaleo("/booking/v1/reservations", token)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data.encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/reservations/schema":
            try:
                schema = get_schema("/booking/v1/reservations", list_key="reservations")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(schema, indent=2).encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/bookings":
            try:
                token = get_access_token()
                data = fetch_data_from_apaleo("/booking/v1/bookings", token)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data.encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))      

        elif path == "/bookings/schema":
            try:
                schema = get_schema("/booking/v1/bookings", list_key="bookings")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(schema, indent=2).encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/folios":
            try:
                token = get_access_token()
                data = fetch_data_from_apaleo("/finance/v1/folios", token)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data.encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/folios/schema":
            try:
                schema = get_schema("/finance/v1/folios", list_key="folios")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(schema, indent=2).encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/properties":
            try:
                token = get_access_token()
                data = fetch_data_from_apaleo("/inventory/v1/properties", token)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data.encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/properties/schema":
            try:
                schema = get_schema("/inventory/v1/properties", list_key="properties")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(schema, indent=2).encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/unit-groups":
            try:
                token = get_access_token()
                data = fetch_data_from_apaleo("/inventory/v1/unit-groups", token)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data.encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/unit-groups/schema":
            try:
                schema = get_schema("/inventory/v1/unit-groups", list_key="unitGroups")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(schema, indent=2).encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/units":
            try:
                token = get_access_token()
                data = fetch_data_from_apaleo("/inventory/v1/units", token)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data.encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/units/schema":
            try:
                schema = get_schema("/inventory/v1/units", list_key="units")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(schema, indent=2).encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/sources":
            try:
                token = get_access_token()
                data = fetch_data_from_apaleo("/booking/v1/types/sources", token)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data.encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/sources/schema":
            try:
                schema = get_schema("/booking/v1/types/sources", list_key="sources")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(schema, indent=2).encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/services":
            try:
                token = get_access_token()
                data = fetch_data_from_apaleo("/rateplan/v1/services", token)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data.encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/services/schema":
            try:
                schema = get_schema("/rateplan/v1/services", list_key="services")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(schema, indent=2).encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/capture-policies":
            try:
                token = get_access_token()
                data = fetch_data_from_apaleo('/settings/v1/capture-policies', token)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data.encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        elif path == "/capture-policies/schema":
            try:
                schema = get_schema("/settings/v1/capture-policies", list_key="capturePolicies")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(schema, indent=2).encode("utf-8"))
            except Exception as e:
                self.send_error(500, str(e))

        else:
            self.send_error(404, "Not Found")

if __name__ == "__main__":
    port = 8000
    server_address = ("", port)
    httpd = HTTPServer(server_address, ApaleoHandler)
    print(f"Running at: http://localhost:{port}")
    httpd.serve_forever()
