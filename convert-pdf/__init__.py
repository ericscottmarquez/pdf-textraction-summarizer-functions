import os
import pypdfium2 as pdfium
from azure.storage.blob import BlobServiceClient
import pymongo
import azure.functions as func
import time
import logging
from io import BytesIO
import requests
import uuid
import os
from azure.functions import HttpResponse

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:

        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================
        # SETUP SECTION
        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================

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
        filename = filename.replace(".pdf", "")

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

        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================
        # BLOB UPLOADING SECTION
        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================

        text_to_upload = ""
        for page_num, page in enumerate(pdf):
            textpage = page.get_textpage()
            text_all = textpage.get_text_range()
            text_to_upload += "\n" + text_all

        # Convert the string data to bytes
        text_to_upload_bytes = text_to_upload.encode('utf-8')

        # Create a BytesIO object from the bytes data
        buffer_text_to_upload = BytesIO(text_to_upload_bytes)

        blob_name = clean_name(folder_name + f'{filename.strip() + ".txt"}')
        logging.info(f"blob_name: {blob_name}")
        blob_name_pdf_file = clean_name(folder_name + f'{filename.strip() + ".pdf"}')
        logging.info(f"blob_name_pdf_file: {blob_name_pdf_file}")


        # Check if a file with the same name already exists in the blob storage
        # blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        # blob_client_upload_raw_pdf = blob_service_client.get_blob_client(container=container_name, blob=blob_name_pdf_file)


        try:
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            if blob_client.exists():
                return func.HttpResponse("You have uploaded a file with that name already. Upload another file or change the file name to upload again.", status_code=400)
            blob_client_upload_raw_pdf = blob_service_client.get_blob_client(container=container_name, blob=blob_name_pdf_file)
            if blob_client_upload_raw_pdf.exists():
                return func.HttpResponse("You have uploaded a pdf with that name already. Upload another file or change the file name to upload again.", status_code=400)
            logging.info(f"got blob client: {blob_client}")

        except Exception as e:
            logging.exception(f"Error getting blob client at page {filename}: {str(e)}")
            return func.HttpResponse(f"Error getting blob client at page {filename} " + str(e), status_code=500)
        
        try:
            blob_client.upload_blob(buffer_text_to_upload, overwrite=True)
            blob_client_upload_raw_pdf.upload_blob(pdf_bytes, overwrite=True)
            
            logging.info(f"Blobs Uploaded!")
            # todo: upload a picture of the first page of the pdf to the blob container:
            # ...
        except Exception as e:
            logging.exception(f"Error forming blob for {filename}: {str(e)}")
            return func.HttpResponse(f"Error forming blob for {filename} " + str(e), status_code=500)
        
        # Step 5: Get the URL of the blob and save it in MongoDB
        blob_url = f"https://pdfsummarizer.blob.core.windows.net/converted-pdfs/{folder_name}{filename}"
        logging.info(f"blob_url: {blob_url}")

        try:
            collection.insert_one({"user_id": user_id, "filename": filename, "blob_url": blob_url})
        except Exception as e:
            return func.HttpResponse(str(e), status_code=500)
        
        while True:
            if blob_client.exists() and blob_client_upload_raw_pdf.exists():
                logging.info(f"The blobs have been successfully uploaded and are available.")
                break
            else:
                logging.info(f"The blob is not available yet. Sleeping for 3 seconds before checking again.")
                time.sleep(3)
        
        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================
        # INDEXER SECTION
        # Initiate the indexer run to populate azure search with newly added data
        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================

        try:
            # Replace with your service name and admin key
            service_name = "cognitive-search-pdf-summarizer"
            admin_key = os.getenv("OPENAI_SEARCH_KEY")

            url = f"https://{service_name}.search.windows.net/indexers/test-indexer-4-indexer/run?api-version=2020-06-30"
            headers = {
                "Content-Type": "application/json",
                "api-key": admin_key
            }

            response = requests.post(url, headers=headers)
            
            if response.status_code == 202:
                logging.info("Indexer run initiated successfully.")
            else:
                logging.error(f"Failed to initiate indexer run. Status code: {response.status_code}, Error message: {response.text}")
            
            try:
                # Check the status of the indexer
                status_url = f"https://{service_name}.search.windows.net/indexers/test-indexer-4-indexer/status?api-version=2020-06-30"
                while True:
                    status_response = requests.get(status_url, headers=headers)
                    status_data = status_response.json()

                    if status_data['lastResult']['status'] == 'success':
                        logging.info("Indexing completed successfully.")
                        break
                    elif status_data['lastResult']['status'] in ['failed', 'transientFailure']:
                        logging.error(f"Indexing failed. Status: {status_data['lastResult']['status']}, Error message: {status_data['lastResult']['errorMessage']}")
                        return func.HttpResponse(f"Indexing failed: {status_data['lastResult']['errorMessage']}", status_code=500)
                    
                    logging.info("Indexing in progress. Sleeping for 3 seconds before checking again.")
                    time.sleep(3)
            except Exception as e:
                logging.exception(f"Error while checking indexer status: {str(e)}")
                return func.HttpResponse(f"Error while checking indexer status: {str(e)}", status_code=500)

        except Exception as e:
            logging.exception(f"Error initiating indexer run: {str(e)}")
            return func.HttpResponse(f"Error initiating indexer run: {str(e)}", status_code=500)
        
        # ================================================================================================================================================================================================
        # CHUNK INDEXER SECTION
        # ================================================================================================================================================================================================
        try:
            # Replace with your service name and admin key
            service_name = "cognitive-search-pdf-summarizer"
            admin_key = os.getenv("OPENAI_SEARCH_KEY")

            url = f"https://{service_name}.search.windows.net/indexers/test-indexer-4-indexer-chunk/run?api-version=2020-06-30"
            headers = {
                "Content-Type": "application/json",
                "api-key": admin_key
            }

            response = requests.post(url, headers=headers)
            
            if response.status_code == 202:
                logging.info("Indexer chunk run initiated successfully.")
            else:
                logging.error(f"Failed to initiate indexer chunk run. Status code: {response.status_code}, Error message: {response.text}")
            
            try:
                # Check the status of the indexer
                status_url = f"https://{service_name}.search.windows.net/indexers/test-indexer-4-indexer-chunk/status?api-version=2020-06-30"
                while True:
                    status_response = requests.get(status_url, headers=headers)
                    status_data = status_response.json()

                    if status_data['lastResult']['status'] == 'success':
                        logging.info("Indexing chunk completed successfully.")
                        break
                    elif status_data['lastResult']['status'] in ['failed', 'transientFailure']:
                        logging.error(f"Indexing chunk failed. Status: {status_data['lastResult']['status']}, Error message: {status_data['lastResult']['errorMessage']}")
                        return func.HttpResponse(f"Indexing chunk failed: {status_data['lastResult']['errorMessage']}", status_code=500)
                    
                    logging.info("Indexing chunk in progress. Sleeping for 3 seconds before checking again.")
                    time.sleep(3)
            except Exception as e:
                logging.exception(f"Error while checking indexer status: {str(e)}")
                return func.HttpResponse(f"Error while checking indexer chunk status: {str(e)}", status_code=500)

        except Exception as e:
            logging.exception(f"Error initiating indexer chunk run: {str(e)}")
            return func.HttpResponse(f"Error initiating indexer chunk run: {str(e)}", status_code=500)
        
        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================
        # END INDEXER SECTION
        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================

        return HttpResponse('file uploaded!', status_code=200)

    except Exception as e:
        # Return an error status code and the error message if there is any exception
        return func.HttpResponse(str(e), status_code=500)
