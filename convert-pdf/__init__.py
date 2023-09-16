import os
import pypdfium2 as pdfium
from azure.storage.blob import BlobServiceClient
import pymongo
import azure.functions as func
import time
import logging
# from io import BytesIO


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Step 0: Setup MongoDB Connection using the connection string from .env
        mongodb_cnx_str = os.environ["mongodb_atlas_cnx_str"]
        client = pymongo.MongoClient(mongodb_cnx_str)
        db = client.get_database('pdf_summarizer')  # replace with your database name
        collection = db.get_collection('pdf')  # replace with your collection name
        # log the connection string to the console
        logging.info(collection)

        # Step 1: Accept a PDF file and a unique userId
        pdf_file = req.files['pdf']
        # log the pdf file
        logging.info(f"PDF File: {pdf_file}")

        user_id = req.form['userId']

        # open pdf file from request stream as if it were a file on disk
        # pdf_bytes = open(pdf_file, 'rb')
        pdf_bytes = pdf_file.read()

        pdf = pdfium.PdfDocument(pdf_bytes)
        # file_name = "extracted_pdfium2.txt"
        # file = open(file_name, 'w')

        # log the userId
        logging.info(user_id)

        connection_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        # log the connection string to the console
        logging.info(connection_str)
        
        # Step 2: Connect to a storage blob container and create a new folder with userId
        blob_service_client = BlobServiceClient.from_connection_string(connection_str)
        # log the blob service client to the console with a description of the message:
        logging.info(f"Blob service client: {blob_service_client}")

        container_name = "converted-pdfs"
        folder_name = os.path.splitext(pdf_file.name)[0] + f'_{user_id}/'
        # log the folder name and a description of the message:
        logging.info(f"Folder name: {folder_name}")

        time.sleep(5)  # Wait for 5 seconds to ensure that the container folder is created


        for page_num, page in enumerate(pdf):
            textpage = page.get_textpage()
            text_all = textpage.get_text_range()
            # file.write(text_all)
            try:
                blob_name = folder_name + f'{page_num}.txt'
            except Exception as e:
                logging.exception(f"Error forming blob name at page {page_num}: {str(e)}")
                continue  # Skip to next iteration if this step fails

            try:
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            except Exception as e:
                logging.exception(f"Error getting blob client at page {page_num}: {str(e)}")
                continue  # Skip to next iteration if this step fails

            try:
                blob_client.upload_blob(text_all, overwrite=True)
            except Exception as e:
                logging.exception(f"Error uploading blob at page {page_num}: {str(e)}")
                continue  # Skip to next iteration if this step fails

        # Step 5: Get the URL of the blob and save it in MongoDB
        blob_url = f"https://pdfsummarizer.blob.core.windows.net/converted-pdfs/{folder_name}"
        collection.insert_one({"user_id": user_id, "name": folder_name, "blob_url": blob_url})
        
        # Step 6: Return a success status code
        return func.HttpResponse("Success", status_code=200)

    except Exception as e:
        # Return an error status code and the error message if there is any exception
        return func.HttpResponse(str(e), status_code=500)
