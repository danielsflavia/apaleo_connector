import os
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse
import requests
from dotenv import load_dotenv
from auth import get_access_token  # IMPORT FROM auth.py

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
                <head><title>Apaleo Connector</title></head>
                <body>
                    <h1>Apaleo API Connector</h1>
                    <ul>
                        <li><a href="/reservations"> View Reservations </a></li>
                        <li><a href="/folios"> View Folios </a></li>
                        <li><a href="/properties"> View Properties </a></li>
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

        else:
            self.send_error(404, "Not Found")

if __name__ == "__main__":
    port = 8000
    server_address = ("", port)
    httpd = HTTPServer(server_address, ApaleoHandler)
    print(f"Running at: http://localhost:{port}")
    httpd.serve_forever()
