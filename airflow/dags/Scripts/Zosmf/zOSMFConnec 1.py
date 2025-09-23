import requests
from requests.auth import HTTPBasicAuth

# Replace with your actual z/OSMF details
# base_url = "https:192.86.32.87:10443/get/zosmf/restfiles/ds/"
# base_url = "https:192.86.32.87:10443/ibm/api/explorer/#!/Dataset_APIs/GetListDataSets"
#base_url = "https:192.86.32.87:10443/ibm/api/explorer/#!/Dataset_APIs/GetReadDataSet_DatasetName"
base_url = "https://192.86.32.87:10443/zosmf/restjobs/jobs?prefix=CG*"
#base_url = "https://192.86.32.87:10443/zosmf/api/explorer/#!/Jobs_APIs/GetListJobs"
dataset_pattern = "CGDEVSK.*"
username = "IBMUSER"
password = "capgem75"

# Full URL
#url = f"{base_url}/{dataset_pattern}"

# Make the GET request
response = requests.get(
    base_url,
    auth=HTTPBasicAuth(username, password),
    headers={"Accept": "application/json"},
    verify=False  # Set to True if using valid SSL certs
)

# Handle response
if response.status_code == 200:
    datasets = response.json().get("items", [])
    for ds in datasets:
        print(ds.get("dsname"))
else:
    print(f"Error {response.status_code}: {response.text}")



