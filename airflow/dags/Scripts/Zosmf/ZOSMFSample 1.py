import requests
from requests.auth import HTTPBasicAuth
import urllib3

# Disable SSL warnings (only for testing)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# API endpoint
# url = "https://192.86.32.87:10443/zosmf/restjobs/jobs"
url = "https://192.86.32.87:10443/zosmf/restfiles/ds?dslevel=CG*"
#url = "https://192.86.32.87:10443/zosmf/restfiles/ds/-(VPWRKF)/CGFSSTV.OUTPUT2"

# Your z/OSMF credentials
username = "CGDEVDS"
password = "Capgem20"

# Headers
headers = {
    "Accept": "application/json"
}

try:
    response = requests.get(
        url,
        headers=headers,
        auth=HTTPBasicAuth(username, password),
        verify=False  # Only for self-signed certs
    )
    response.raise_for_status()
    data = response.json()
    print("API Response:", data)
except requests.exceptions.RequestException as e:
    print("Error fetching data:", e)