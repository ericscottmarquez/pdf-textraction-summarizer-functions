import os
from azure.storage.blob import BlobServiceClient
import pymongo
import azure.functions as func
import logging

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Step 0: Setup MongoDB Connection using the connection string from .env
        mongodb_cnx_str = os.environ["mongodb_atlas_cnx_str"]
        client = pymongo.MongoClient(mongodb_cnx_str)
        db = client.get_database('pdf_summarizer')  # replace with your database name
        collection = db.get_collection('pdf')  # replace with your collection name
        logging.info(collection)

        # Step 1: Accept a unique userId from the form data
        user_id = req.form['userId']
        logging.info(f"userId: {user_id}")

        def clean_name(name):
            # Add a function to remove or replace invalid characters here
            # For simplicity, we're just replacing spaces with underscores
            return name.replace(" ", "_")

        connection_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        logging.info(f"connection_str: {connection_str}")

        # Step 2: Connect to a storage blob container
        blob_service_client = BlobServiceClient.from_connection_string(connection_str)
        logging.info(f"Blob service client: {blob_service_client}")

        container_name = "converted-pdfs"
        folder_name = clean_name(f'{user_id.strip()}' + "/")

        # Step 3: List all files in the folder matching the userId
        container_client = blob_service_client.get_container_client(container_name)
        blobs_list = container_client.list_blobs(name_starts_with=folder_name)
        
        files = [blob.name for blob in blobs_list]

        # If no files are found, return a message indicating this
        if not files:
            return func.HttpResponse("No files found for the given userId.", status_code=200)

        # Step 4: Return the list of files in the HTTP response
        return func.HttpResponse(str(files), status_code=200)

    except Exception as e:
        # Return an error status code and the error message if there is any exception
        return func.HttpResponse(str(e), status_code=500)
