import logging
import os

import azure.functions as func
import json
import traceback
from ..utils import storage_helpers
from ..utils import processing

def main(msg: func.QueueMessage) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))
    
    file_name = msg.get_body().decode('utf-8')
    logging.info(f"Processing queue item: {file_name}…")

    # Getting settings
    STORAGE_CONNECTION_STRING = os.getenv("yulpcsstorageaccount_STORAGE")
    CONTAINER_NAME = os.getenv("STORAGE_CONTAINER_NAME")
    API_KEY = os.getenv("API_KEY")
    API_ENDPOINT = os.getenv("API_ENDPOINT")
    TABLE_NAME = os.getenv("STORAGE_TABLE_NAME")
    SQL_SERVER_CONNECTION_STRING = os.getenv("SQL_SERVER_CONNECTION_STRING")
    MAP_API_ENDPOINT = os.getenv("MAP_API_ENDPOINT")
    # Getting file from storage
    #file_path = storage_helpers.download_blob(CONTAINER_NAME, file_name, STORAGE_CONNECTION_STRING)
    message = msg.get_body().decode('utf-8')
    query = json.loads(message)
    try:
        if(query.get('type') == 'api'):
            #Do the api thing
            processing.get_business_list(query, TABLE_NAME, API_KEY, API_ENDPOINT, STORAGE_CONNECTION_STRING)
        elif(query.get('type') == 'web'):
            location = query.get('location')
            location_hash = processing.encodeStr(location)

            #storage_helpers.update_status(TABLE_NAME, location_hash, query.get('id'), query.get('url'), 'processing', STORAGE_CONNECTION_STRING)
            processing.get_reviews(query, TABLE_NAME, STORAGE_CONNECTION_STRING, API_ENDPOINT)
        elif(query.get('type') == 'welphome'):
            url = 'http://x-welp.azurewebsites.net/'            
            chunk_blob_name = query.get('blob_name')
            chunk_container_name = query.get('container_name')
            processing.get_welp_business_list(chunk_blob_name, chunk_container_name, API_KEY, API_ENDPOINT, STORAGE_CONNECTION_STRING, url)
        elif(query.get('type') == 'welpreview'):
            processing.get_welp_reviews(query, TABLE_NAME, STORAGE_CONNECTION_STRING, API_ENDPOINT, SQL_SERVER_CONNECTION_STRING, MAP_API_ENDPOINT)

        #processed_doc = "HTML FILE"

        #storage_helpers.upload_blob(CONTAINER_NAME, file_name, processed_doc, STORAGE_CONNECTION_STRING)
    
    except Exception:
        logging.error(traceback.format_exc())

