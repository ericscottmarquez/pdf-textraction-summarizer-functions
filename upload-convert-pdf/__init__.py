import os
from azure.storage.blob import BlobServiceClient
from PyPDF2 import PdfFileReader
from PIL import Image
import io
import pymongo
from azure import func

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Step 0: Setup MongoDB Connection using the connection string from .env
        mongodb_cnx_str = os.environ["mongodb_atlas_cnx_str"]
        client = pymongo.MongoClient(mongodb_cnx_str)
        db = client.get_database('your_database_name')  # replace with your database name
        collection = db.get_collection('your_collection_name')  # replace with your collection name

        # Step 1: Accept a PDF file and a unique userId
        pdf_file = req.files['pdf']
        user_id = req.form['userId']

        connection_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        
        # Step 2: Connect to a storage blob container and create a new folder with userId
        blob_service_client = BlobServiceClient.from_connection_string(connection_str)
        container_name = "pdfsummarizer"
        folder_name = os.path.splitext(pdf_file.filename)[0] + f'_{user_id}/'
        
        # Step 3 and 4: Convert the PDF to a series of images and upload each image to the new folder
        pdf_reader = PdfFileReader(pdf_file)
        for page_num in range(pdf_reader.getNumPages()):
            page = pdf_reader.getPage(page_num)
            image_writer = Image.new('RGB', (page.mediaBox.getWidth(), page.mediaBox.getHeight()), (255, 255, 255))
            image_writer.save(io.BytesIO(), format='JPEG')
            
            blob_name = folder_name + f'{page_num}.jpeg'
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            blob_client.upload_blob(io.BytesIO(), overwrite=True)
        
        # Step 5: Get the URL of the blob and save it in MongoDB
        blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{folder_name}"
        collection.insert_one({"user_id": user_id, "name": folder_name, "blob_url": blob_url})
        
        # Step 6: Return a success status code
        return func.HttpResponse("Success", status_code=200)
    
    except Exception as e:
        # Return an error status code and the error message if there is any exception
        return func.HttpResponse(str(e), status_code=500)