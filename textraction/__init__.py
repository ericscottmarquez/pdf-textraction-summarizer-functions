import os
import time
import logging
import azure.functions as func
from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from azure.cognitiveservices.vision.computervision.models import OperationStatusCodes
from msrest.authentication import CognitiveServicesCredentials
from azure.storage.blob import BlobServiceClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Get environment variables
    subscription_key = os.environ["VISION_KEY"]
    endpoint = os.environ["VISION_ENDPOINT"]
    connection_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]

    # Initialize Computer Vision client
    computervision_client = ComputerVisionClient(endpoint, CognitiveServicesCredentials(subscription_key))

    # Initialize Blob Service Client
    blob_service_client = BlobServiceClient.from_connection_string(connection_str)

    # Get blob folder URL from the request
    blob_folder_url = req.params.get('blob_folder_url')
    if not blob_folder_url:
        return func.HttpResponse(
            "Please pass a blob_folder_url on the query string",
            status_code=400
        )
    
    # Extract container name and folder name from the URL
    parts = blob_folder_url.split("/")
    container_name = parts[3]  # Adjust based on your URL structure
    folder_name = parts[4]     # Adjust based on your URL structure

    # Get a list of all blobs in the specified folder
    blob_list = blob_service_client.get_container_client(container_name).list_blobs(name_starts_with=folder_name)
    
    # Iterate over all blobs and perform OCR
    for blob in blob_list:
        # Form the full blob URL
        blob_url = f"{blob_folder_url}/{blob.name}"
        
        # Perform OCR to extract text
        read_response = computervision_client.read(blob_url, raw=True)
        read_operation_location = read_response.headers["Operation-Location"]
        operation_id = read_operation_location.split("/")[-1]

        # Call the "GET" API and wait for it to retrieve the results 
        while True:
            read_result = computervision_client.get_read_result(operation_id)
            if read_result.status not in ['notStarted', 'running']:
                break
            time.sleep(1)

        # Compile the detected text and save it as a .txt file in the Blob Storage
        if read_result.status == OperationStatusCodes.succeeded:
            result = []
            for text_result in read_result.analyze_result.read_results:
                for line in text_result.lines:
                    result.append(line.text)
            
            # Create a text file and upload it to Blob Storage
            txt_content = "\n".join(result)
            txt_blob_name = blob.name.replace(".jpg", ".txt")  # Replace with the appropriate image extension
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=txt_blob_name)
            blob_client.upload_blob(txt_content, overwrite=True)
        else:
            logging.error(f"Failed to extract text from blob: {blob_url}")

    return func.HttpResponse(
        "Text extraction and upload completed",
        status_code=200
    )
