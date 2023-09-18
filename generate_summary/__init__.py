import os
from azure.storage.blob import BlobServiceClient
import pymongo
import azure.functions as func
import time
import logging
import requests
import uuid
import os
from azure.functions import HttpResponse
import openai

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:

        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================
        # SETUP SECTION
        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================

        mongodb_cnx_str = os.environ["mongodb_atlas_cnx_str"]
        client = pymongo.MongoClient(mongodb_cnx_str)
        db = client.get_database('pdf_summarizer')  # replace with your database name
        collection = db.get_collection('pdf')  # replace with your collection name
        # log the connection string to the console
        logging.info(collection)

        user_id = req.form['userId']
        logging.info(f"userId: {user_id}")

        filename = req.form['fileName']
        logging.info(f"fileName: {filename}")
        filename = filename.replace(".pdf", "")

        summary_level = req.form['summaryLevel']
        logging.info(f"summary_level: {summary_level}")

        if not filename or not summary_level:
            return HttpResponse("Please provide both filename and summary_level", status_code=400)

        def clean_name(name):
            # Add a function to remove or replace invalid characters here
            # For simplicity, we're just replacing spaces with underscores
            return name.replace(" ", "_")

        connection_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        logging.info(f"connection_str: {connection_str}")

        blob_service_client = BlobServiceClient.from_connection_string(connection_str)
        logging.info(f"Blob service client: {blob_service_client}")

        folder_name = clean_name(f'{user_id.strip()}' + "/")

        # log the folder name and a description of the message:
        logging.info(f"Folder name: {folder_name}")

        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================
        # GPT CHAT COMPLETIONS SECTION
        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================

        # You would formulate your prompt here based on the pdf_name and summary_level
        # prompt = f"You have access to multiple pdf files which are uploaded to your indexed search. Specifically, summarize the {filename} content and no other content at a {summary_level} level of understanding. The emphasis here is on the summary level."
        # prompt = f"At a {summary_level} level of understanding, tell me about the text that discusses: {text_to_upload[:3000]}"
        # prompt = f"Summarize the document {filename.strip()}.txt at a {summary_level} level."
        # prompt = f"Please locate the document with the exact filename '{filename.strip()}.txt'. Summarize its contents accurately at a {summary_level} level of detail. Ensure you are referring to the correct document before generating the summary."
        prompt = f"Summarize the document {filename.strip()} at a {summary_level} level."

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

        return HttpResponse(summary_text, status_code=200)

    except Exception as e:
        # Return an error status code and the error message if there is any exception
        return func.HttpResponse(str(e), status_code=500)
