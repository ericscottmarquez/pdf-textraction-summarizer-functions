import os
from azure.storage.blob import BlobServiceClient
from PyPDF2 import PdfFileReader
from PIL import Image
import io
import pymongo
import azure.functions as func
# from pdf2image import convert_from_bytes
import time
import logging
import fitz  # PyMuPDF

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
        logging.info(pdf_file)
        user_id = req.form['userId']

        # log the userId
        logging.info(user_id)

        connection_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        # log the connection string to the console
        logging.info(connection_str)
        
        # Step 2: Connect to a storage blob container and create a new folder with userId
        blob_service_client = BlobServiceClient.from_connection_string(connection_str)
        # log the blob service client to the console with a description of the message:
        logging.info(f"Blob service client: {blob_service_client}")

        container_name = "pdfsummarizer"
        folder_name = os.path.splitext(pdf_file.name)[0] + f'_{user_id}/'
        # log the folder name and a description of the message:
        logging.info(f"Folder name: {folder_name}")

        time.sleep(5)  # Wait for 5 seconds to ensure that the container folder is created

        try:
            # Get the byte data from the FileStorage object
            pdf_bytes = pdf_file.read()
            pdf_file.seek(0)  # Reset the file pointer to the start

            # Load the PDF bytes data using PyMuPDF
            pdf = fitz.open(io.BytesIO(pdf_bytes))

            # Initialize an empty list to store images
            images = []

            for page_num in range(len(pdf)):
                # Get the page
                page = pdf[page_num]

                # Convert the page to a pixmap (a kind of image)
                pix = page.get_pixmap()

                # Convert the pixmap to a PIL Image
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                
                # Add the image to the list
                images.append(img)

        except Exception as e:
            # Log the error message to the console with a description:
            logging.info(f"Error at PyMuPDF processing: {str(e)}")



        for page_num, image in enumerate(images):
            try:
                image_io = io.BytesIO()
                image.save(image_io, format='JPEG')
            except Exception as e:
                logging.exception(f"Error saving image to BytesIO at page {page_num}: {str(e)}")
                continue  # Skip to next iteration if this step fails

            try:
                blob_name = folder_name + f'{page_num}.jpeg'
            except Exception as e:
                logging.exception(f"Error forming blob name at page {page_num}: {str(e)}")
                continue  # Skip to next iteration if this step fails

            try:
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            except Exception as e:
                logging.exception(f"Error getting blob client at page {page_num}: {str(e)}")
                continue  # Skip to next iteration if this step fails

            try:
                blob_client.upload_blob(image_io.getvalue(), overwrite=True)
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
