import os
import pymongo
import azure.functions as func
import time
import logging
import requests
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

        connection_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        logging.info(f"connection_str: {connection_str}")

        # ================================================================================================================================================================================================
        # ================================================================================================================================================================================================
        # INDEXER SECTION
        # Initiate the indexer run to populate azure search with newly added data
        # IN PRODUCTION, THIS SHOULD BE A GLOBAL INDEXER THAT RUNS ON A 30 SECOND SCHEDULE FOR ALL USERS, WHERE IT SHOWS ALL USERS A TIMER DIAL OF WHEN NEXT INDEX WILL RUN FOR DATA TO BE AVAILABLE IN OPENAI SEARCH
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
