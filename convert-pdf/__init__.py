import os
import pypdfium2 as pdfium
from azure.storage.blob import BlobServiceClient
import pymongo
import azure.functions as func
import time
import logging
# from io import BytesIO
from io import BytesIO
import requests
import uuid
import os
from azure.functions import HttpRequest, HttpResponse
import openai


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

        summary_level = req.form['summaryLevel']
        logging.info(f"summary_level: {summary_level}")

        if not filename or not summary_level:
            return HttpResponse("Please provide both filename and summary_level", status_code=400)

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

        # Check if a file with the same name already exists in the blob storage
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        if blob_client.exists():
            return func.HttpResponse("You have uploaded a file with that name already. Upload another file or change the file name to upload again.", status_code=400)

        try:
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            logging.info(f"got blob client: {blob_client}")

        except Exception as e:
            logging.exception(f"Error getting blob client at page {filename}: {str(e)}")
            return func.HttpResponse(f"Error getting blob client at page {filename} " + str(e), status_code=500)
        
        try:
            blob_client.upload_blob(buffer_text_to_upload, overwrite=True)
            logging.info(f"Blob Uploaded!")
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
            if blob_client.exists():
                logging.info(f"The blob has been successfully uploaded and is available.")
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


        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================
        # GPT CHAT COMPLETIONS SECTION
        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================

        # You would formulate your prompt here based on the pdf_name and summary_level
        # prompt = f"You have access to multiple pdf files which are uploaded to your indexed search. Specifically, summarize the {filename} content and no other content at a {summary_level} level of understanding. The emphasis here is on the summary level."
        prompt = f"At a {summary_level} level of understanding, tell me about the text that discusses: {text_to_upload[:3000]}"
        # prompt = f"Summarize the document {folder_name + filename.strip()}.txt at a {summary_level} level."
        # prompt = f"Please locate the document with the exact filename '{filename.strip()}.txt'. Summarize its contents accurately at a {summary_level} level of detail. Ensure you are referring to the correct document before generating the summary."

        logging.info(f"prompt: {prompt}")

        # Wait for 5 seconds to allow the files to be ready AND INDEXER TO COMPLETE
        # time.sleep(5)

        response = None
        try:
            openai.api_type = "azure"
            openai.api_version = "2023-08-01-preview"
            openai.api_base = "https://pdf-summarizer.openai.azure.com/" # Add your endpoint here
            openai.api_key = os.getenv("OPENAI_API_KEY") # Add your OpenAI API key here
            deployment_id = "pdf-summarizer-model-deployment" # Add your deployment ID here
            # Azure Cognitive Search setup
            search_endpoint = "https://cognitive-search-pdf-summarizer.search.windows.net" # Add your Azure Cognitive Search endpoint here
            search_key = os.getenv("OPENAI_SEARCH_KEY") # Add your Azure Cognitive Search admin key here
            # search_index_name = "pdf-blob-name-index" # Add your Azure Cognitive Search index name here

            def setup_byod(deployment_id: str) -> None:
                """Sets up the OpenAI Python SDK to use your own data for the chat endpoint.
                :param deployment_id: The deployment ID for the model to use with your own data.
                To remove this configuration, simply set openai.requestssession to None.
                """
                class BringYourOwnDataAdapter(requests.adapters.HTTPAdapter):
                    def send(self, request, **kwargs):
                        request.url = f"{openai.api_base}/openai/deployments/{deployment_id}/extensions/chat/completions?api-version={openai.api_version}"
                        return super().send(request, **kwargs)

                session = requests.Session()

                # Mount a custom adapter which will use the extensions endpoint for any call using the given `deployment_id`
                session.mount(
                    prefix=f"{openai.api_base}/openai/deployments/{deployment_id}",
                    adapter=BringYourOwnDataAdapter()
                )
                openai.requestssession = session

            setup_byod(deployment_id)

            session_id = str(uuid.uuid4())
            response = openai.ChatCompletion.create(
                messages=[
                    {"role": "user", "content": prompt}
                ],
                # session_id=session_id,
                deployment_id=deployment_id,
                dataSources=[
                    {
                        "type": "AzureCognitiveSearch",
                        "parameters": {
                            "endpoint": search_endpoint,
                            "key": search_key,
                            "indexName": "test-indexer-4",
                        }
                    }
                ]
            )

            logging.info(f"response: {response}")

        except Exception as e:
            logging.exception(f"Error running openai section of script: {str(e)}")
            return HttpResponse(str(e), status_code=500)
        
            # Validate that the response contains the expected data
        summary_text = None
        if 'choices' in response and response['choices'] and 'message' in response['choices'][0] and 'content' in response['choices'][0]['message']:
            summary_text = response['choices'][0]['message']['content']
        else:
            logging.exception(f"Error capturing openai generated response value")
            raise ValueError("The response from OpenAI does not contain the expected data")

        # summary_text_bytes = summary_text.encode('utf-8')
        # buffer_summary_text = BytesIO(summary_text_bytes)

        # summarized_blob_name = clean_name(folder_name + f'{filename.strip()}-summary.txt')
        # logging.info(f"summarized_blob_name: {summarized_blob_name}")

        # try:
        #     blob_client = blob_service_client.get_blob_client(container=container_name, blob=summarized_blob_name)
        # except Exception as e:
        #     logging.exception(f"Error getting blob client at page {filename}: {str(e)}")
        #     return func.HttpResponse(f"Error getting blob client at page {filename} " + str(e), status_code=500)
        
        # try:
        #     blob_client.upload_blob(buffer_summary_text, overwrite=True)
        # except Exception as e:
        #     logging.exception(f"Error forming blob for {summarized_blob_name}: {str(e)}")
        #     return func.HttpResponse(f"Error forming blob for {summarized_blob_name} " + str(e), status_code=500)

        # Stream the response back (here it's returned in one go, but you could modify to stream)
        return HttpResponse(summary_text, status_code=200)

    except Exception as e:
        # Return an error status code and the error message if there is any exception
        return func.HttpResponse(str(e), status_code=500)
