import logging
import azure.functions as func
from azure.storage.queue import QueueClient
from azure.storage.queue import QueueServiceClient
import openai
import json
import pymongo
import requests
import os
import logging


# Set up GPT-3
openai.organization = None
openai.api_key = os.environ["OPENAI_API_KEY"]

MAX_TOKENS = 16000
CHUNK_SIZE = MAX_TOKENS - 6000

mongodb_cnx_str = os.environ["mongodb_atlas_cnx_str"]
client = pymongo.MongoClient(mongodb_cnx_str)
db = client.get_database('pdf_summarizer')  # replace with your database name
summaries_collection = db.get_collection('summaries')  # replace with your collection name
# log the connection string to the console
logging.info(summaries_collection)




def summarize_chunk(chunk):
    response = openai.Completion.create(
        engine="gpt-3.5-turbo",
        prompt=f"Please summarize the following text:\n{chunk}",
        max_tokens=2000  # You can adjust this number based on how short you want the summary
    )
    return response.choices[0].text.strip()

def summarize(blob, userId):
    # Azure Queue Storage setup
    connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]  # Store this in Azure Function App Settings or Key Vault
    queue_service_client = QueueServiceClient.from_connection_string(connection_string)
    queue_name = f"pdf-summarizer-queue-{userId}".lower()
    queue_client = queue_service_client.get_queue_client(queue_name)

    # Check if the queue exists, and if not, create it
    try:
        queue_client.get_queue_properties()
    except:
        queue_service_client.create_queue(queue_name)

    # Get the content from the blob which is a blob url
    # Fetch the content from the blob URL
    response = requests.get(blob)
    response.raise_for_status()  # Raise an exception if the request failed
    content = response.text
    # Log the first 500 characters
    logging.info(content[:500])
        
    # Split the content into chunks
    chunks = [content[i:i+CHUNK_SIZE] for i in range(0, len(content), CHUNK_SIZE)]
    
    summarized_text = ""

    # Summarizing each chunk
    for chunk in chunks:
        prompt = {
            "messages": [
                {
                    "role": "system",
                    "content": "You are a helpful assistant. Summarize the following text."
                },
                {
                    "role": "user",
                    "content": chunk
                }
            ]
        }
        total_chunks_length = len(chunks)
        chunk_index = chunks.index(chunk)
        percentage_complete = round((chunk_index + 1) / total_chunks_length * 100)
        queue_message = {"progress": f"{percentage_complete}%"}  # Adjust message format if necessary
        queue_client.send_message(json.dumps(queue_message))
        
        response = openai.ChatCompletion.create(model="gpt-3.5-turbo", messages=prompt["messages"], max_tokens=100)
        summarized_text += response['choices'][0]['message']['content'] + "\n\n"
    summaries_collection.insert_one({"user_id": userId, "blob": blob, "summary": summarized_text})
    return summarized_text

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    blob = req.params.get('blob')
    userId = req.params.get('userId')

    if not blob:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            blob = req_body.get('blob')

    if not userId:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            userId = req_body.get('userId')

    if blob:
        summarized_text = summarize(blob, userId)
        return func.HttpResponse(summarized_text, status_code=200)
    else:
        return func.HttpResponse(
            "Please pass a blob and userId on the query string or in the request body",
            status_code=400
        )
