import requests
from requests.auth import HTTPBasicAuth
import urllib3

# Disable SSL warnings (only for testing)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# # API endpoint
# url = "https://192.86.32.87:10443/zosmf/restjobs/jobs"
#
# # Your z/OSMF credentials
# username = "CGDEVDS"
# password = "Capgem20"
#
# # Headers
# headers = {
#     "Accept": "application/json"
# }
#
# try:
#     response = requests.get(
#         url,
#         headers=headers,
#         auth=HTTPBasicAuth(username, password),
#         verify=False  # Only for self-signed certs
#     )
#     response.raise_for_status()
#     data = response.json()
#     print("API Response:", data)
# except requests.exceptions.RequestException as e:
#     print("Error fetching data:", e)

# To get content of a dataset from mainframe
volser = "VPWRKH"  # Replace with your volume serial
dataset_name = "DCC.FET.CPY(TEST1)"  # Replace with your data set name
username = "CGDEVDS"
password = "Capgem20"

# === Construct the URL ===
url = f"https://192.86.32.87:10443/zosmf/restfiles/ds/-({volser})/{dataset_name}"

# === Make the GET request ===
response = requests.get(
    url,
    auth=HTTPBasicAuth(username, password),
    headers={"Accept": "text/plain"},
    verify=False  # Set to True if using valid SSL certs
)

# === Handle the response ===
if response.status_code == 200:
    print("Data set contents:")
    with open("copybook.txt", "w") as file:
        file.write(response.text)
else:
    print(f"Failed to retrieve data set. Status code: {response.status_code}")
    print("Response:", response.text)
