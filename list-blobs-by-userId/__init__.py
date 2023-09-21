import os
from azure.storage.blob import BlobServiceClient
import azure.functions as func
import json
import logging

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # SETUP SECTION
        connection_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        logging.info(f"connection_str: {connection_str}")

        blob_service_client = BlobServiceClient.from_connection_string(connection_str)
        logging.info(f"Blob service client: {blob_service_client}")

        user_id = req.params.get('userId')
        if not user_id:
            try:
                req_body = req.get_json()
            except ValueError:
                pass
            else:
                user_id = req_body.get('userId')

        logging.info(f"userId: {user_id}")

        if not user_id:
            return func.HttpResponse("Please pass a userId in the request body", status_code=400)

        container_name = "converted-pdfs"  # replace with your container name

        def clean_name(name):
            return name.replace(" ", "_")

        folder_name = clean_name(f'{user_id.strip()}/')
        logging.info(f"Folder name: {folder_name}")

        # Listing blobs
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=folder_name)

        blobs = [
            {
                "blobName": blob.name,
                "blobUrl": container_client.url + "/" + blob.name
            }
            for blob in blob_list
        ]

        if blobs:
            logging.info(f"Blobs in folder {folder_name}: {json.dumps(blobs, indent=4)}")
            return func.HttpResponse(json.dumps(blobs), status_code=200)
        else:
            return func.HttpResponse(f"No blobs found in folder {folder_name}", status_code=404)

    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)
