# client.py
import requests
import json

url = 'http://localhost:8001/api/v1/otl/metadata' # The API endpoint URL
try:
    response = requests.post(url, data=json.dumps({
        "keys":[
            "/v1/observation/iers",
            "/v1/observatory"
        ]
    }))
    # Raise an exception for bad status codes (4xx or 5xx)
    response.raise_for_status()

    # The response data is often in JSON format, which can be converted to a Python dictionary
    data = response.json()
    print(json.dumps(data, indent=4))
    print(f"Status Code: {response.status_code}")

except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")
