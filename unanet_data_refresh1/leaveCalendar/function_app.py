import os
import azure.functions as func
import logging
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient

# Load `UNANET_LOGIN_URL` from Azure Function Application Settings
UNANET_LOGIN_URL = os.getenv("UNANET_LOGIN_URL")
USERNAME = os.getenv("UNANET_USERNAME")
PASSWORD = os.getenv("UNANET_PASSWORD")
AZURE_STORAGE_ACCOUNT_NAME = "appapiunanetfetch"
CONTAINER_NAME = "scm-releases"
BLOB_SAS_TOKEN = os.getenv("SAS_TOKEN")
# Static configuration values (will be parameterized later)

UNANET_LEAVE_REQUESTS_URL = "https://oteemo.unanet.biz/platform/rest/leave-requests?page=0&pageSize=1000&start=2024-01-01&end=2025-12-31&status=INUSE&status=SUBMITTED&status=DISAPPROVED&status=APPROVING&status=COMPLETED&status=LOCKED&status=CANCELED&statusStart=2024-01-01&statusEnd=2024-12-31"
UNANET_PEOPLE_LIST_URL = "https://oteemo.unanet.biz/platform/rest/people?page=0&pageSize=2000&active=true"

LEAVE_REQUESTS_BLOB_NAME = "leave_requests.csv"
PEOPLE_LIST_BLOB_NAME = "people_list.csv"

# Function to retrieve Unanet token
def get_unanet_token():
    logging.info("Requesting token from Unanet")
    payload = {"username": USERNAME, "password": PASSWORD}
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    
    try:
        response = requests.post(UNANET_LOGIN_URL, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()["token"]
    except requests.RequestException as e:
        logging.error(f"Error requesting token: {e}")
        raise

# Function to fetch data from Unanet API
def fetch_unanet_data(token, url):
    logging.info(f"Fetching data from {url}")
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("items", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        raise

# Function to transform data
def transform_data(data):
    try:
        return pd.json_normalize(data)
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise

# Function to upload data to Azure Blob Storage
def upload_to_azure_blob(csv_data, blob_name):
    try:
        blob_service_client = BlobServiceClient(
            account_url=f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net?{BLOB_SAS_TOKEN}"
        )
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(csv_data, overwrite=True)
        logging.info(f"Data uploaded to {blob_name} successfully.")
    except Exception as e:
        logging.error(f"Error uploading to Azure Blob Storage: {e}")
        raise

# Azure Function App
app = func.FunctionApp()

@app.route(route="unanetRefreshApp", auth_level=func.AuthLevel.ANONYMOUS)
def unanet_refresh_app(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing Unanet refresh request")
    
    try:
        # Step 1: Get the Unanet authentication token
        token = get_unanet_token()

        # Step 2: Fetch, transform, and upload leave requests
        leave_requests = fetch_unanet_data(token, UNANET_LEAVE_REQUESTS_URL)
        upload_to_azure_blob(transform_data(leave_requests).to_csv(index=False), LEAVE_REQUESTS_BLOB_NAME)

        # Step 3: Fetch, transform, and upload people list
        people_list = fetch_unanet_data(token, UNANET_PEOPLE_LIST_URL)
        upload_to_azure_blob(transform_data(people_list).to_csv(index=False), PEOPLE_LIST_BLOB_NAME)

        return func.HttpResponse("Data fetched and uploaded successfully.", status_code=200)
    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
