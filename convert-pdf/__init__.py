import os
import pypdfium2 as pdfium
from azure.storage.blob import BlobServiceClient
import pymongo
import azure.functions as func
import time
import logging
# from io import BytesIO
from io import BytesIO

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
        logging.info(f"userId: {user_id}")

        filename = req.form['fileName']
        logging.info(f"fileName: {filename}")

        # open pdf file from request stream as if it were a file on disk
        # pdf_bytes = open(pdf_file, 'rb')
        pdf_bytes = pdf_file.read()
        logging.info(f"pdf read! ")

        pdf = pdfium.PdfDocument(pdf_bytes)
        logging.info(f"pdf converted! ")
        # file_name = "extracted_pdfium2.txt"
        # file = open(file_name, 'w')
        def clean_name(name):
            # Add a function to remove or replace invalid characters here
            # For simplicity, we're just replacing spaces with underscores
            return name.replace(" ", "_")

        connection_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        logging.info(f"connection_str: {connection_str}")

        # Step 2: Connect to a storage blob container and create a new folder with userId
        blob_service_client = BlobServiceClient.from_connection_string(connection_str)
        logging.info(f"Blob service client: {blob_service_client}")

        container_name = "converted-pdfs"
        folder_name = clean_name(f'{user_id.strip()}' + "/")

        # log the folder name and a description of the message:
        logging.info(f"Folder name: {folder_name}")

        text_to_upload = ""
        for page_num, page in enumerate(pdf):
            textpage = page.get_textpage()
            text_all = textpage.get_text_range()
            text_to_upload += "\n" + text_all

        # Convert the string data to bytes
        text_to_upload_bytes = text_to_upload.encode('utf-8')

        # Create a BytesIO object from the bytes data
        buffer_text_to_upload = BytesIO(text_to_upload_bytes)

        blob_name = clean_name(folder_name + f'{filename}.txt')
 
        try:
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        except Exception as e:
            logging.exception(f"Error getting blob client at page {filename}: {str(e)}")
            return func.HttpResponse(f"Error getting blob client at page {filename} " + str(e), status_code=500)
        
        try:
            # todo: use pdf_file.name along with text_all to create one large text file instead of 
            # creating a new blob for each page which is how this currently works.
            blob_client.upload_blob(buffer_text_to_upload, overwrite=True)
            # todo: upload a picture of the first page of the pdf to the blob container:
            # ...
        except Exception as e:
            logging.exception(f"Error forming blob for {filename}: {str(e)}")
            return func.HttpResponse(f"Error forming blob for {filename} " + str(e), status_code=500)
        
        # Step 5: Get the URL of the blob and save it in MongoDB
        blob_url = f"https://pdfsummarizer.blob.core.windows.net/converted-pdfs/{folder_name}"
        logging.info(f"blob_url: {blob_url}")

        try:
            collection.insert_one({"user_id": user_id, "filename": filename, "blob_url": blob_url})
        except Exception as e:
            return func.HttpResponse(str(e), status_code=500)

        # Step 6: Return a success status code
        return func.HttpResponse("Success", status_code=200)

    except Exception as e:
        # Return an error status code and the error message if there is any exception
        return func.HttpResponse(str(e), status_code=500)
