import azure.functions as func
import logging
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
import os

# Set logging configuration
logging.basicConfig(level=logging.INFO)

# Environment variables for configuration
USERNAME = os.getenv("UNANET_USERNAME")
if not USERNAME:
    logging.error("UNANET_USERNAME is not set. Please check environment variables.")
    raise ValueError("Missing environment variable: UNANET_USERNAME")
PASSWORD = os.getenv("UNANET_PASSWORD")
if not PASSWORD:
    logging.error("UNANET_PASSWORD is not set. Please check environment variables.")
    raise ValueError("Missing environment variable: UNANET_PASSWORD")
AZURE_STORAGE_ACCOUNT_NAME = "appapiunanetfetch"
CONTAINER_NAME = "scm-releases"
PLANNED_TIME_BLOB_NAME = "planned_matrix.csv"
PROJECTS_BLOB_NAME = "projects.csv"
UNANET_LOGIN_URL = "https://oteemo.unanet.biz/platform/rest/login"
PLANNING_TIME_URL_TEMPLATE = "https://oteemo.unanet.biz/platform/rest/planning/time/{id}"
PROJECT_DETAILS_URL_TEMPLATE = "https://oteemo.unanet.biz/platform/rest/projects/{id}"

# Define the SAS token (ensure this is kept secure and not hard-coded in production)
SAS_TOKEN = os.getenv("BLOB_SAS_TOKEN")

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

# Fetch Planned Time Data
def fetch_planned_time(token, project_id):
    logging.info(f"Fetching planned time data for project ID: {project_id}")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = PLANNING_TIME_URL_TEMPLATE.format(id=project_id)

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.warning(f"Error fetching planned time data for project ID {project_id}: {e}")
        return None

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

@app.route(route="unanet-fetch-planned-time", auth_level=func.AuthLevel.ANONYMOUS)
def unanet_fetch_planned_time(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing Unanet fetch planned time request.")
    try:
        token = get_unanet_token()
        planned_time_data = []
        failure_count = 0
        max_failures = 200
        start_project_id = 2000
        project_id = start_project_id

        while failure_count < max_failures:
            data = fetch_planned_time(token, project_id)
            if data:
                planned_time_data.append(data)
                failure_count = 0
            else:
                failure_count += 1
            project_id += 1

        if planned_time_data:
            planned_time_df = pd.json_normalize(planned_time_data)
            upload_to_azure_blob(planned_time_df.to_csv(index=False), PLANNED_TIME_BLOB_NAME)

        return func.HttpResponse("Planned time data fetched and uploaded successfully.", status_code=200)
    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return func.HttpResponse(f"Error processing request: {e}", status_code=500)

@app.route(route="unanet-fetch-projects", auth_level=func.AuthLevel.ANONYMOUS)
def unanet_fetch_projects(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing Unanet fetch projects request.")

    try:
        # Get the authentication token
        token = get_unanet_token()

        # Fetch project details for projects up to project ID 500
        projects_data = []

        for project_id in range(1, 501):  # Adjust range as needed
            try:
                # Fetch project details for each project
                project = fetch_project_details(token, project_id)
                if project:
                    projects_data.append(project)
            except Exception as e:
                logging.warning(f"Failed to fetch project details for project ID {project_id}: {e}")
                continue

        # Transform and save project details to CSV
        if projects_data:
            projects_df = transform_data(projects_data)
            upload_to_azure_blob(projects_df.to_csv(index=False, sep="|"), PROJECTS_BLOB_NAME)

        logging.info("All project details fetched and uploaded successfully.")
        return func.HttpResponse("Project details fetched and uploaded successfully.", status_code=200)

    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return func.HttpResponse(f"Error processing request: {e}", status_code=500)