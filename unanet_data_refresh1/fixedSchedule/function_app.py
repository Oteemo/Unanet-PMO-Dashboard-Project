import os
import azure.functions as func
import logging
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient

# Set logging configuration
logging.basicConfig(level=logging.INFO)

# Environment variables for configuration
USERNAME = os.getenv("UNANET_USERNAME")
PASSWORD = os.getenv("UNANET_PASSWORD")
AZURE_STORAGE_ACCOUNT_NAME = "appapiunanetfetch"
CONTAINER_NAME = "scm-releases"
UNANET_LOGIN_URL = "https://oteemo.unanet.biz/platform/rest/login"
FIXED_PRICE_ITEMS_TEMPLATE = "https://oteemo.unanet.biz/platform/rest/projects/{id}/fixed-price-items?page=1&pageSize=1500"
PROJECT_URL_TEMPLATE = "https://oteemo.unanet.biz/platform/rest/projects/{id}"
FIXED_PRICE_SCHEDULE_BLOB_NAME = "fixedpriceschedulesheet.csv"

# Define the SAS token (ensure this is kept secure and not hard-coded in production)
SAS_TOKEN = os.getenv("BLOB_SAS_TOKEN")

# Helper: Get Unanet Token
def get_unanet_token():
    logging.info("Requesting token from Unanet.")
    payload = {"username": USERNAME, "password": PASSWORD}
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    
    try:
        response = requests.post(UNANET_LOGIN_URL, json=payload, headers=headers)
        response.raise_for_status()
        token = response.json().get("token")
        if not token:
            raise ValueError("Token not found in response.")
        logging.info("Token retrieved successfully.")
        return token
    except requests.exceptions.RequestException as e:
        logging.error(f"Error requesting token: {e}")
        raise


# Helper: Fetch Unanet Data
def fetch_unanet_data(token, url):
    logging.info(f"Fetching data from URL: {url}")
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {url}: {e}")
        raise


# Fetch Projects and Fixed Price Items
def fetch_projects_and_items(token, limit=500):
    """
    Fetch projects up to a given ID limit and their associated fixed-price items.
    Args:
        token (str): The authentication token for Unanet API.
        limit (int): The maximum project ID to fetch.
    Returns:
        list: A combined list of project and fixed-price item data.
    """
    logging.info(f"Fetching projects and fixed-price items up to project ID {limit}.")
    combined_data = []

    for project_id in range(1, limit + 1):  # Iterate from ID 1 to limit
        try:
            # Fetch project details
            project_url = PROJECT_URL_TEMPLATE.format(id=project_id)
            project = fetch_unanet_data(token, project_url)

            # Fetch fixed-price items for the project
            fixed_price_url = FIXED_PRICE_ITEMS_TEMPLATE.format(id=project_id)
            fixed_price_items = fetch_unanet_data(token, fixed_price_url).get("items", [])

            # Combine project and fixed-price item data
            for item in fixed_price_items:
                combined_data.append({
                    "project_id": project_id,
                    "code": project.get("code"),
                    "billing_currency": project.get("billingCurrency", {}).get("code"),
                    "project_org": project.get("projectOrg", {}).get("code"),
                    "project_currency": project.get("projectCurrency", {}).get("code"),
                    "owning_org": project.get("owningOrg", {}).get("code"),
                    "item_key": item.get("key"),
                    "task_key": item.get("taskKey"),
                    "post_history_key": item.get("postHistoryKey"),
                    "billable_post_history_key": item.get("billablePostHistoryKey"),
                    "description": item.get("description"),
                    "bill_date": item.get("billDate"),
                    "bill_on_completion": item.get("billOnCompletion"),
                    "amount": item.get("amount"),
                    "revenue_recognition_method": item.get("revenueRecognitionMethod"),
                    "schedule": item.get("schedule"),
                })
            logging.info(f"Fetched project ID {project_id} and associated items.")
        except requests.exceptions.RequestException as e:
            logging.warning(f"Failed to fetch data for project ID {project_id}: {e}")
            continue

    return combined_data


# Transform Data to DataFrame
def transform_data(data):
    try:
        df = pd.json_normalize(data)
        logging.info("Data transformed into DataFrame successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise


# Upload to Azure Blob Storage
def upload_to_azure_blob(csv_data, blob_name):
    logging.info(f"Uploading data to Azure Blob Storage: {blob_name}")
    try:
        blob_service_client = BlobServiceClient(
            account_url=f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net?{SAS_TOKEN}"
        )
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(csv_data, overwrite=True)
        logging.info(f"Uploaded successfully to {blob_name}.")
    except Exception as e:
        logging.error(f"Error uploading to Azure Blob Storage: {e}")
        raise


# Azure Function App
app = func.FunctionApp()

@app.route(route="unanetFetchFixedPriceSchedule", auth_level=func.AuthLevel.ANONYMOUS)
def unanet_fetch_fixed_price_schedule(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing Unanet fetch fixed price schedule request.")

    try:
        # Get the authentication token
        token = get_unanet_token()

        # Fetch projects and associated fixed-price items up to project ID 500
        combined_data = fetch_projects_and_items(token, limit=500)

        # Transform and save data to CSV
        combined_df = transform_data(combined_data)
        upload_to_azure_blob(combined_df.to_csv(index=False), FIXED_PRICE_SCHEDULE_BLOB_NAME)

        logging.info("All project and item data fetched and uploaded successfully.")
        return func.HttpResponse("Project and item data fetched and uploaded successfully.", status_code=200)

    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return func.HttpResponse(f"Error processing request: {e}", status_code=500)
