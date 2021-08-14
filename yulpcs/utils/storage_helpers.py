import json
import logging
import uuid
import pyodbc
from azure.storage.blob import BlobServiceClient
from azure.cosmosdb.table.tableservice import TableService
from azure.core.exceptions import ResourceNotFoundError


# Creates a blob service client
def create_blob_service_client(connection_string):
    blob_service_client = None
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        logging.info("Created blob service client.")
    except Exception as e:
        logging.error(f"Could not create blob service client: {e}")
    return blob_service_client

# Downloads the specified blob
def download_blob(container_name, blob_name, connection_string):
    blob_service_client = create_blob_service_client(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    try:
        if blob_client.exists():
            downloads_folder = "downloads"
            #local_path = downloads_folder + '/' + blob_name
            #if not os.path.exists(downloads_folder):
            #    os.makedirs(downloads_folder)
            #with open(local_path, "wb") as f:
            stream = blob_client.download_blob()
                #f.write(stream.readall())
            return str(stream.readall(), 'utf-8')
        else:
            logging.error(f"Blob {blob_name} doesn't exist.")
    except ResourceNotFoundError:
        logging.error(f"The blob {blob_name} was not found.")
    return None

def createContainerId():
    return str(uuid.uuid4())


# Creates a blob in blob storage from bytes
def create_container(container_name, connection_string):
    blob_service_client = create_blob_service_client(connection_string)
        
    #blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    try:
        blob_service_client.create_container (container_name)
        logging.info(f"Created container {container_name} successfully.")
        return True
    except Exception as e:
        logging.error(f"Error creating blob {container_name}: {e}")
    return False


# Creates a blob in blob storage from bytes
def upload_blob(container_name, blob_name, data, connection_string):
    blob_service_client = create_blob_service_client(connection_string)
    if not container_name:
        container_name = createContainerId()
        blob_service_client.create_container (container_name)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    try:
        blob_client.upload_blob(data=data)
        logging.info(f"Created blob {blob_name} successfully.")
        return True
    except Exception as ex:
        logging.error(f"Error creating blob {blob_name}: {ex}")
        try:
            blob_client.delete_blob()
            blob_client.upload_blob(data=data)
            logging.warning(f"Blob {blob_name} already exists, deleted it.")
            logging.info(f"Created blob {blob_name} successfully.")
            return True
        except Exception as e:
            logging.error(f"Error creating blob {blob_name}: {e}")
    return False


def blob_exists(container_name, blob_name, connection_string):
    blob_service_client = create_blob_service_client(connection_string)
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        res = blob_client.exists()
        logging.info(f"Blob exists {blob_name}.")
        return res
    except Exception:
        try:
            logging.warning(f"Blob {blob_name} doesn't exists.")
            return False
        except Exception as e:
            logging.error(f"Error creating blob {blob_name}: {e}")
    return False

###############
# TABLE STORAGE
###############


# Creates an Azure Table Storage service
def create_table_service(connection_string):
    table_service = None
    try:
        table_service = TableService(connection_string=connection_string)
    except Exception as e:
        logging.error(f"Could not instantiate table service: {e}")
    return table_service


# Creates an entity if it doesn't exist, updates its status if it does
def update_status(table_name, location_hash, row_key, item_name, status, connection_string):
    table_service = create_table_service(connection_string)
    try:
        entity = {
            "PartitionKey": location_hash, 
            "RowKey": row_key,
            "status": status,
            "url": item_name
        }
        logging.info("entity being stored: {}".format(entity))
        table_service.insert_or_replace_entity(table_name, entity)
        logging.info(f"Set status for item {item_name} to {status}.")
        return True
    except Exception as e:
        logging.error(f"Could not insert or update entity in table {table_name}:{e}")
        return False

def query_status(table_name, location_hash, row_key, connection_string):
    table_service = create_table_service(connection_string)
    try:
        logging.info("querying status: {}".format(row_key))
        res = table_service.get_entity(table_name, location_hash, row_key)
        logging.info(f"Status found: {res}.")
        if res:
            return res
        else: 
            return False
    except Exception as e:
        logging.error(f"Status not found {location_hash} - {row_key}:{e}")
        return False

def query_review(table_name, business_id, row_key, connection_string):
    table_service = create_table_service(connection_string)
    try:
        logging.info("querying review: {}".format(row_key))
        res = table_service.get_entity(table_name, business_id, row_key)
        logging.info(f"review found: {res}.")
        if res:
            return res
        else: 
            return False
    except Exception as e:
        logging.error(f"Status not found {business_id} - {row_key}:{e}")
        return False


def update_container_query(table_name, location_hash, query_hash, query, container_guid,  connection_string):
    table_service = create_table_service(connection_string)
    try:
        entity = {
            "PartitionKey": location_hash, 
            "RowKey": query_hash,
            "query": query,
            "container_guid":container_guid
        }
        logging.info("entity being stored: {}".format(entity))
        table_service.insert_or_replace_entity(table_name, entity)
        logging.info(f"Store query hash {query_hash} to {query}.")
        return True
    except Exception as e:
        logging.error(f"Could not insert or update entity in table {table_name}:{e}")
        return False

def query_container_exists(table_name, location_hash, query_hash, connection_string):
    table_service = create_table_service(connection_string)
    try:
        logging.info("querying for container: {}".format(query_hash))
        res = table_service.get_entity(table_name, location_hash, query_hash)
        logging.info(f"Container: {res}.")
        if res:
            return res
        else: 
            return False
    except Exception as e:
        logging.error(f"Entity not founf {location_hash} - {query_hash}:{e}")
        return False

def update_review(table_name, businessId, businessId_page, url, total, business, reviews, connection_string):
    table_service = create_table_service(connection_string)
    try:
        entity = {
            "PartitionKey": businessId, 
            "RowKey": businessId_page,
            "Url": url,
            "Total": total,
            "business": json.dumps(business),
            "reviews": json.dumps(reviews)
        }
        logging.info("entity being stored: {}, {}, {}".format(entity['PartitionKey'], entity['RowKey'], len(reviews) if reviews else 0))
        table_service.insert_or_merge_entity(table_name, entity)
        logging.info(f"Store business {businessId_page} with {len(reviews) if reviews else 0} reviews.")
        return True
    except Exception as e:
        logging.error(f"Could not insert or update entity in table {table_name}:{e}")
        return False


def insert_review(business_id, state_code, lat, lon, business, reviews, sql_connection_string): 
    try:
        # Connection string
        cnxn = pyodbc.connect(sql_connection_string)
            #'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +server+';DATABASE='+database+';UID='+username+';PWD=' + password)
        cursor = cnxn.cursor()
    
        # Prepare the stored procedure execution script and parameter values
        storedProc = "Exec [dbo].[submit_review] @Id = ?, @BusinessId = ?, @StateCode = ?, @Latitude = ?, @Longitude = ?, @BusinessJson = ?, @ReviewsJson = ?"
        params = (-1, business_id, state_code, lat, lon, json.dumps(business), json.dumps(reviews))
    
        # Execute Stored Procedure With Parameters
        cursor.execute( storedProc, params )
    
        # Iterate the cursor
        row = cursor.fetchone()
        while row:
            # Print the row
            logging.info(str(row[0]))
            row = cursor.fetchone()
        cursor.commit()
        # Close the cursor and delete it
        cursor.close()
        del cursor
    
        # Close the database connection
        cnxn.close()
    
    except Exception as e:
        logging.info("Error: %s" % e)
        raise e

def query_business_review(business_id, sql_connection_string): 
    try:
        # Connection string
        cnxn = pyodbc.connect(sql_connection_string)
            #'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +server+';DATABASE='+database+';UID='+username+';PWD=' + password)
        cursor = cnxn.cursor()
    
        # Prepare the stored procedure execution script and parameter values
        storedProc = "Exec [dbo].[get_business_review] @BusinessId = ?"
        params = (business_id)
    
        # Execute Stored Procedure With Parameters
        cursor.execute( storedProc, params )
    
        # Iterate the cursor
        row = cursor.fetchone()

        # Close the cursor and delete it
        cursor.close()
        del cursor
    
        # Close the database connection
        cnxn.close()

        return row
    
    except Exception as e:
        logging.info("Error: %s" % e)
        raise e