import azure.functions as func
import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io
import os

# Azure Storage Config
AZURE_STORAGE_ACCOUNT_NAME = "appapiunanetfetch"
CONTAINER_NAME = "scm-releases"
PLANNED_MATRIX_BLOB = "planned_matrix.csv"
LABOR_CATEGORY_BLOB = "Labor Category.csv"

# Define the SAS token (ensure this is kept secure and not hard-coded in production)
SAS_TOKEN = os.getenv("BLOB_SAS_TOKEN")

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Read CSV from Azure Blob Storage
def read_csv_from_blob(blob_name, skip_first_row=False):
    try:
        blob_service_client = BlobServiceClient(
            account_url=f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net?{SAS_TOKEN}"
        )
        blob_client = blob_service_client.get_container_client(CONTAINER_NAME).get_blob_client(blob_name)
        downloaded_blob = blob_client.download_blob().readall()

        # Convert blob data to a DataFrame
        raw_data = io.StringIO(downloaded_blob.decode("utf-8"))

        # Load CSV and strip spaces from column names
        df = pd.read_csv(raw_data, skiprows=1 if skip_first_row else 0)
        df.columns = df.columns.str.strip()  #Remove leading/trailing spaces from column names

        return df
    
    except Exception as e:
        logging.error(f"Error reading blob {blob_name}: {e}")
        raise

# Write CSV to Azure Blob Storage
def write_csv_to_blob(dataframe, blob_name):
    try:
        blob_service_client = BlobServiceClient(
            account_url=f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net?{SAS_TOKEN}"
        )
        blob_client = blob_service_client.get_container_client(CONTAINER_NAME).get_blob_client(blob_name)
        blob_client.upload_blob(dataframe.to_csv(index=False), overwrite=True)
        logging.info(f"Successfully uploaded updated data to blob {blob_name}.")
    except Exception as e:
        logging.error(f"Error writing blob {blob_name}: {e}")
        raise

# Azure Function App
app = func.FunctionApp()

@app.route(route="update-bill-rate", auth_level=func.AuthLevel.ANONYMOUS)
def update_bill_rate(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing request to update billRate.")

    try:
        # Read planned_matrix.csv and labor_category.csv from Blob Storage
        planned_matrix_df = read_csv_from_blob(PLANNED_MATRIX_BLOB, skip_first_row=False)
        labor_category_df = read_csv_from_blob(LABOR_CATEGORY_BLOB, skip_first_row=True)

        logging.info("CSV files loaded successfully.")

        # Ensure column names are stripped of spaces
        labor_category_df.columns = labor_category_df.columns.str.strip()

        # Standardize column names in labor_category.csv to match planned_matrix.csv
        labor_category_df = labor_category_df.rename(
            columns={
                "Person Key": "person.key",
                "Project Key": "project.key",
                "Labor Category": "laborCategory.name",
                "Bill Rate": "new_billRate",
                "Begin Date": "beginDate",
                "End Date": "endDate"
            }
        )

        # Drop any rows where critical fields are missing
        labor_category_df.dropna(subset=["person.key", "project.key", "laborCategory.name", "beginDate", "endDate", "new_billRate"], inplace=True)

        # Convert columns to appropriate types for proper merging
        labor_category_df["person.key"] = labor_category_df["person.key"].astype("Int64")
        labor_category_df["project.key"] = labor_category_df["project.key"].astype("Int64")
        planned_matrix_df["person.key"] = planned_matrix_df["person.key"].astype("Int64")
        planned_matrix_df["project.key"] = planned_matrix_df["project.key"].astype("Int64")

        # Convert billRate from string to numeric
        labor_category_df["new_billRate"] = (
            labor_category_df["new_billRate"]
            .astype(str)
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False) 
            .astype(float)
        )

        # Convert "Begin Date" and "End Date" to datetime format for accurate matching
        labor_category_df["beginDate"] = pd.to_datetime(labor_category_df["beginDate"])
        labor_category_df["endDate"] = pd.to_datetime(labor_category_df["endDate"])
        planned_matrix_df["beginDate"] = pd.to_datetime(planned_matrix_df["beginDate"])
        planned_matrix_df["endDate"] = pd.to_datetime(planned_matrix_df["endDate"])

        # Keeping the highest billRate per key combination
        labor_category_df = (
            labor_category_df.groupby(["person.key", "project.key", "laborCategory.name", "beginDate", "endDate"], as_index=False)
            .agg({"new_billRate": "max"})  # Keep the highest billRate
        )

        # Preserve all original rows while only updating billRate
        updated_planned_matrix = planned_matrix_df.merge(
            labor_category_df,
            on=["person.key", "project.key", "laborCategory.name", "beginDate", "endDate"],
            how="left"
        )

        # Update billRate only when new_billRate is NOT zero
        updated_planned_matrix["billRate"] = updated_planned_matrix["new_billRate"].combine_first(updated_planned_matrix["billRate"])

        # Drop the helper column after merging
        updated_planned_matrix.drop(columns=["new_billRate"], inplace=True)

        # Ensure strict row preservation
        final_row_count = updated_planned_matrix.shape[0]
        original_row_count = planned_matrix_df.shape[0]

        if final_row_count != original_row_count:
            logging.error(f"Row count mismatch: Original={original_row_count}, Updated={final_row_count}")
            return func.HttpResponse(f"Row count mismatch: Original={original_row_count}, Updated={final_row_count}", status_code=500)

        # Write updated data back to Blob Storage
        write_csv_to_blob(updated_planned_matrix, PLANNED_MATRIX_BLOB)
        logging.info("CSV file updated and uploaded successfully.")

        return func.HttpResponse("CSV file updated and uploaded successfully.", status_code=200)

    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return func.HttpResponse(f"Error processing request: {e}", status_code=500)
