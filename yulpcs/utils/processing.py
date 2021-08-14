import traceback
import base64
import logging
from os import path
import uuid
from azure.storage.blob._shared.base_client import create_configuration

import requests
from utils.storage_helpers import blob_exists, create_container, download_blob, insert_review, query_business_review, query_container_exists, query_review, query_status, update_container_query, update_review, update_status, upload_blob
import glom
import time
from utils.yelp_spider import YelpSpider
import urllib.parse as urlparse
import json
from ..utils import processing

DEFAULT_LIMIT = 50

def get_business_list(element, TABLE_NAME, API_KEY, API_ENDPOINT, connection_string):

    yelp_spider = YelpSpider()
    term = element.get('term')
    location = element.get('location')
    offset = element.get('offset')
    total = element.get('total_business')
    start = time.time()
    page = yelp_spider.query_api(term, location, offset, API_KEY)
    end = time.time()
    get = end-start
    logging.info("get_page: {} ".format(get))
    #logging.info("get_page: ", get)
    businesses = glom.glom(page, "businesses")
    total = page.get('total')
    if not businesses:  
        logging.info(u'No businesses for {0} in {1} found.'.format(term, location))
        return

    location_hash = processing.encodeStr(location)

    for item in businesses:
        item_status = query_status(TABLE_NAME, location_hash, item['id'], connection_string)
        parsed = urlparse.urlparse(item['url'])
        item_url = parsed.scheme + "://" + parsed.netloc + parsed.path
        if(item_status):
            if(item_status['status'] == 'processed'):
                logging.info(u'Id: {0} Already processed for {1} in found.'.format(item['id'], item_url))
                continue
        
        msg = {
            'id':item['id'],
            'type':'web',
            'term':term,
            'location':location,
            'url': parsed.scheme + "://" + parsed.netloc + parsed.path,
            'offset':0,
            'review_count': item['review_count'],
            "meta_data": {
                "business_name": item['name'],
                "business_address": item['location']['display_address'],
                "business_categories": [],
                "total_reviews": item['review_count'],
                "business_rating": item['rating'],
                "total_reviews_per_language": [
                    {
                        "code": "en",
                        "count": item['review_count']
                    }
                ]
            },
            "total_reviews": item['review_count'],
            "reviews":[]
        }
        for cat in item['categories']:
            msg['meta_data']['business_categories'].append(cat['title'])
        #check if this process is correct
        #we want to make sure the review for this item does not exist yet
        #if the record exist but does not contain reviews we want to add it to the queue
        business_id = item.get('id')
        row_key = business_id +'_'+str(0)
        review = query_review('welpreviews', business_id, row_key, connection_string)
        reviews = []
        if(review):
            reviews = json.loads(review['reviews'])

        if(not review or len(reviews) == 0):
            logging.info("adding msg to queue")
            logging.info(msg)
            query = json.dumps(msg)
            r = requests.post(url = API_ENDPOINT, json={"query": query})

        current_count = 0
        review_count = int(item['review_count'])
        while(current_count < review_count):
            logging.info("adding msg to queue")
            logging.info(msg)
            msg['offset'] = current_count + 10
            business_id = item.get('id')
            row_key = business_id +'_'+str(msg['offset'])
            review = query_review('welpreviews', business_id, row_key, connection_string)

            if(review):
                reviews = json.loads(review['reviews'])
                logging.info('review found for location {} rowkey {}'.format(review['PartitionKey'], review['Key']))

            if(not review or len(reviews) == 0):
                query = json.dumps(msg)
                r = requests.post(url = API_ENDPOINT, json={"query": query})
                logging.info('Post to url {} with status code {}'.format(r.url, r.status_code))
                current_count = current_count + 10
        #break
        ##queue.append(msg)
        #Make request to http trigger request with msg as the body

    offset = int(offset)
    #logging.info(queue, indent=2)
    if(offset + DEFAULT_LIMIT < total):
        apimsg = {
            'type':'api',
            'term': term,
            'location': location,
            'offset': offset + DEFAULT_LIMIT,
            'total_business': total
        }
        logging.info("adding msg to queue")
        logging.info(apimsg)
        apiquery = json.dumps(apimsg)
        r = requests.post(url = API_ENDPOINT, json={"query": apiquery})
        logging.info('Post to url {} with status code {}'.format(r.url, r.status_code))


def store_details(element, details):
    logging.info("storing details for: {}".format(element))
    logging.info("details {}".format(details))

def encodeStr(name):
    b_name = str.encode(name, 'UTF-8')
    encoded_name = base64.urlsafe_b64encode(b_name)
    return str(encoded_name, 'utf-8')

def get_reviews(element, TABLE_NAME, connection_string, api_endpoint):
    #logging.info(element)
    #return
    yelp_spider = YelpSpider()
    #start_number = element.start_number
    term = element.get('term')
    location = element.get('location')
    offset = element.get('offset')
    total = element.get('total_reviews')
    url = element.get('url')
    logging.info("get_reviews:element")
    logging.info(element)

    container_name = term +'_'+location

    encoded_container_name = encodeStr(container_name)

    location_hash = encodeStr(location)

    

    container = query_container_exists('welpcontainerquery', location_hash, encoded_container_name, connection_string)

    if(container):
        container_id = container.get('container_guid')

    url = url + '?start=' + str(offset) if offset > 0 else url
    encoded_url = encodeStr(url)

    business_id = element.get('id')
    row_key = business_id +'_'+str(offset)
    review = query_review('welpreviews', business_id, row_key, connection_string)
    if(review):        
        reviews = json.loads(review['reviews'])
        if(reviews and len(reviews) > 0):
            logging.info('review found for location {} rowkey {}'.format(review['PartitionKey'], review['Key']))
            if(offset + 10 < total):
                update_status('welpstatus', location_hash, element.get('id'), element.get('url'), 'processed', connection_string)
            return
    
    update_status(TABLE_NAME, location_hash, element.get('id'), element.get('url'), 'processing', connection_string)
    if(blob_exists(container_id, encoded_url, connection_string)):
        page = download_blob(container_id, encoded_url, connection_string)
        page_text = page
        if "Sorry, you’re not allowed to access this page" in page_text:
            page = yelp_spider.get_business_page(url, start_number=offset)
            page_text = page.text
            upload_blob(container_id, encoded_url, page.text, connection_string)
    else:
        page = yelp_spider.get_business_page(url, start_number=offset)
        page_text = page.text
        upload_blob(container_id, encoded_url, page.text, connection_string)


    update_container_query('welpcontainerquery', location_hash, encoded_container_name, container_name, container_id, connection_string)

    details = yelp_spider.get_detail(url, start_number=offset, page_text = page_text)
    #store_details(element, details)
    key = element.get('id') +'_'+str(offset)
    update_review("welpreviews", element.get('id'), key, url, total, details, connection_string)
    if(offset + 10 < total):
        update_status('welpstatus', location_hash, element.get('id'), element.get('url'), 'processed', connection_string)
        # element['offset'] = offset + 10
        # logging.info("adding msg to queue ")
        # logging.info(element)
        # query = json.dumps(element)
        # r = requests.post(url = api_endpoint, json={"query": query})
        # logging.info('Post to url {} with status code {}'.format(r.url, r.status_code))
    #else:


def get_welp_business_list(chunk_blob_name, chunk_container_name, API_KEY, API_ENDPOINT, connection_string, url):

    yelp_spider = YelpSpider()
    

    container_name = 'welpcontainer'

    #encoded_container_name = encodeStr(container_name)

    #location_hash = encodeStr(location)



    container = query_container_exists('welpcontainerquery', 'onlyonelocation', 'welpcontainer', connection_string)

    if(not container):
        create_container(container_name, connection_string)
         #container_id = container.get('container_guid')
    # else:
    #     container_id = False

    # #url = url + '?start=' + str(offset) if offset > 0 else url
    encoded_url = encodeStr(url)

    #LOCAL TESTING ONLY!
    # if(path.exists('./downloads/welphome.html')):
    #     f = open("./downloads/welphome.html", "r")
    #     page = f.read()
    #     page_text = page
    # else:
    #     page = yelp_spider.get_page(url)
    #     with open('./downloads/welphome.html', 'w') as f:
    #         f.write(page)
    #     page_text = page.text
        #upload_blob(container_id, encoded_url, page.text, connection_string)
    if(blob_exists(chunk_container_name, chunk_blob_name, connection_string)):
        page = download_blob(chunk_container_name, chunk_blob_name, connection_string)
        page_text = page
        if "Sorry, you’re not allowed to access this page" in page_text:
            raise Exception("blob does not exist yet")
    else:
        raise Exception("blob does not exist yet")
        #upload_blob(container_name, encoded_url, page.text, connection_string)
    

    businesses = yelp_spider.get_welp_business_list(url, 0, page_text, API_ENDPOINT)
    logging.info('Found {} businesses'.format(len(businesses)))
    for item in businesses:
        business_id = item.get('id')
        #logging.info('review found for location {} rowkey {}'.format(review['PartitionKey'], review['Key']))
        row_key = business_id #+'_'+str(msg['offset'])
        
        query = json.dumps(item)
        r = requests.post(url = API_ENDPOINT, json={"query": query})
        logging.info('Post to url {} with status code {}'.format(r.url, r.status_code))
    

def get_welp_reviews(element, TABLE_NAME, connection_string, api_endpoint, sql_connection_string, MAP_API_ENDPOINT):
    #logging.info(element)
    #return
    try:
        yelp_spider = YelpSpider()
        #start_number = element.start_number
        total = element.get('total_reviews')
        url = element.get('url')
        logging.info("get_reviews:element {}".format(url))
        #logging.info(element)

        container_name = 'welpcontainer'

        encoded_container_name = encodeStr(container_name)

        #location_hash = encodeStr(location)

        business_id = element.get('id')
        business_row = query_business_review(business_id, sql_connection_string)
        if business_row and business_row[5] != '[]':
            return

        #container = query_container_exists('welpcontainerquery', 'onlyonelocation', container_name, connection_string)

        #if(not container):
        #    create_container(container_name, connection_string)

        #url = url #+ '?start=' + str(offset) if offset > 0 else url
        encoded_url = encodeStr(url)

        #row_key = int(datetime.datetime.utcnow().timestamp())#business_id #+'_'+str(offset)
        
        update_status(TABLE_NAME, str.lower(element.get('state')), element.get('id'), element.get('url'), 'processing', connection_string)
        if(blob_exists(container_name, encoded_url, connection_string)):
            page = download_blob(container_name, encoded_url, connection_string)
            page_text = page
            if "Server Error" in page_text:
                page = yelp_spider.get_page(url)
                page_text = page.text
                upload_blob(container_name, encoded_url, page.text, connection_string)
        else:
            page = yelp_spider.get_page(url)
            page_text = page.text
            upload_blob(container_name, encoded_url, page.text, connection_string)


        update_container_query('welpcontainerquery', 'onlyonelocation', container_name, container_name, container_name, connection_string)

        details = yelp_spider.get_welp_detail(url, start_number=0, page_text = page_text)
        #store_details(element, details)
        #key = element.get('id') +'_'+str(offset)
        lat = 0
        lon = 0
        try:
            if(element and element.get('meta_data') and element.get('meta_data').get('business_address')):
                map_url = MAP_API_ENDPOINT.format(element.get('meta_data').get('business_address'))
                data = requests.get(url = map_url)
                jsondata = json.loads(data.text)
                if(data.status_code == 200):
                    lat = jsondata['results'][0]['position']['lat']
                    lon = jsondata['results'][0]['position']['lon']
        except Exception as exception:
        # change proxy if has proxy
            logging.error(exception)
            logging.error("error querying location")

        
        insert_review(element.get('id'), str.lower(element.get('state')), lat, lon, element, details, sql_connection_string)
        #update_review("welpreviews", str.lower(element.get('state')), element.get('id'), url, total, element, details, connection_string)
        
        update_status('welpstatus', str.lower(element.get('state')), element.get('id'), element.get('url'), 'processed', connection_string)
    except Exception as ex:
        logging.error(traceback.format_exc())
        raise ex