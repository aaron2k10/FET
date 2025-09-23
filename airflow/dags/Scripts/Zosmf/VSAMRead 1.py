import requests
from requests.auth import HTTPBasicAuth
import urllib3
import json


# Disable SSL warnings (only for testing)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# API endpoint for job submission
url = "https://192.86.32.87:10443/zosmf/restfiles/ams"

# z/OSMF credentials
username = "IBMUSER"
password = "capgem75"

# JSON body with the JCL file location
# Example for a dataset:
payload = {

  "input": [
    "PRINT INDATASET(CGDEVPB.DCC.SAMPLE.VSAM.KSDS.DATA) CHAR"
  ],
  "JSONversion": 1
  # Optional: "recall": "yesnowait"
}

# Headers
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

try:
    response = requests.put(
        url,
        data=json.dumps(payload),
        headers=headers,
        auth=HTTPBasicAuth(username, password),
        verify=False
    )
    response.raise_for_status()
    print("Job submitted successfully.")
    print("Response:", response.json())
except requests.exceptions.RequestException as e:
    print("Error submitting job:", e)
