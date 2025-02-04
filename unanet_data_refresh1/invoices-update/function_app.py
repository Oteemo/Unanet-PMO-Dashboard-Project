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
INVOICE_URL_TEMPLATE = "https://oteemo.unanet.biz/platform/rest/invoices/{}"
INVOICE_BLOB_NAME = "invoice_data.csv"

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


# Helper: Fetch Unanet Invoice Data
def fetch_all_invoices(token, max_consecutive_misses=10):
    """
    Fetch invoices from Unanet API, iterating until a threshold of consecutive missing invoices is reached.
    Returns:
        list: A list of invoice data.
    """
    invoice_id = 1  # Start from the first invoice
    invoices = []
    consecutive_misses = 0  # Track consecutive missing invoices

    logging.info("Fetching invoices from Unanet.")

    while consecutive_misses < max_consecutive_misses:
        try:
            invoice_url = INVOICE_URL_TEMPLATE.format(invoice_id)
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(invoice_url, headers=headers)

            # If the invoice does not exist, log and continue fetching
            if response.status_code == 404:
                logging.info(f"Invoice ID {invoice_id} not found. Skipping.")
                consecutive_misses += 1  # Increment the missing counter
                invoice_id += 1  # Move to the next invoice ID
                continue

            response.raise_for_status()
            invoice_data = response.json()

            # Append invoice details to the list
            invoices.append(invoice_data)
            logging.info(f"Fetched invoice ID {invoice_id}.")

            invoice_id += 1  # Move to the next invoice ID
            consecutive_misses = 0  # Reset the missing counter since we found an invoice

        except requests.exceptions.RequestException as e:
            logging.warning(f"Failed to fetch invoice ID {invoice_id}: {e}")
            break  # Stop fetching on critical errors

    logging.info(f"Finished fetching invoices. Total invoices retrieved: {len(invoices)}")
    return invoices


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

@app.route(route="unanetFetchInvoices", auth_level=func.AuthLevel.ANONYMOUS)
def unanet_fetch_invoices(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing Unanet invoice fetch request.")

    try:
        # Get the authentication token
        token = get_unanet_token()

        # Fetch all invoices
        invoices = fetch_all_invoices(token, max_consecutive_misses=100)

        # Transform and save data to CSV
        invoice_df = transform_data(invoices)
        upload_to_azure_blob(invoice_df.to_csv(index=False), INVOICE_BLOB_NAME)

        logging.info("All invoices fetched and uploaded successfully.")
        return func.HttpResponse("Invoice data fetched and uploaded successfully.", status_code=200)

    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return func.HttpResponse(f"Error processing request: {e}", status_code=500)
