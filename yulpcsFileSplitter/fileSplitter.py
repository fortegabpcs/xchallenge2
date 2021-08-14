
from logging import exception, info
from os import path
import os

from bs4.element import PageElement, Tag
import requests
from utils.yelp_spider import YelpSpider
import logging
from datetime import datetime
from utils.storage_helpers import *

from bs4 import BeautifulSoup
def split_file(url):
    spider = YelpSpider()
    #page = spider.get_page(url)

    STORAGE_CONNECTION_STRING = ""
    API_KEY = os.getenv("API_KEY")
    API_ENDPOINT = ""
    #API_ENDPOINT = "http://localhost:7071/api/yulpTriggerProcessing"
    STORAGE_CONTAINER_NAME = 'welpchunks100'

    #LOCAL TESTING ONLY!
    if(path.exists('./downloads/welphome.html')):
        f = open("./downloads/welphome.html", "r")
        page = f.read()
        page_text = page
    else:
        page = spider.get_page(url)
        with open('./downloads/welphome.html', 'w') as f:
            f.write(page.text)
        page_text = page.text

    #page_text = page

    try:
        text = page_text

        soup = BeautifulSoup(text, "lxml")

        
        now = datetime.now()
        logging.info("started welp parsing... {}".format(now))

        allBiz = soup.find_all("div",{"class":"bizDiv"})

        u=[]
        l={}
        
        create_container(STORAGE_CONTAINER_NAME, STORAGE_CONNECTION_STRING)

        now = datetime.now()
        logging.info("parsing businesses 1 by 1... {}".format(now))
        chunk_size = 100
        for i in range(0,len(allBiz), chunk_size):
            chunk = allBiz[i:i+chunk_size]
            soup2 = BeautifulSoup()

            html =  soup2.new_tag("html")
            body = soup2.new_tag("body")
            soup2.append(html)
            html.append(body)
            for biz in chunk:
                body.append(biz)

            print("processing items offset: {}".format(str(i)))
            blob_name = 'welphomechunk' + str(i) + '.html'
            if(not blob_exists(STORAGE_CONTAINER_NAME, blob_name, STORAGE_CONNECTION_STRING)):
                upload_blob(STORAGE_CONTAINER_NAME, blob_name, str(soup2), STORAGE_CONNECTION_STRING)
            item = {
                "type": "welphome",
                "blob_name": blob_name,
                "container_name": STORAGE_CONTAINER_NAME
            }
            query = json.dumps(item)
            r = requests.post(url = API_ENDPOINT, json={"query": query})
            if(r.status_code != 200):
                logging.error(r)
            logging.info("processing chunk with offset {}".format(str(i)))
            #with open('./downloads/chunk'+str(i)+'.html', 'w') as f:
            #    f.write(str(soup2))
        
            
            



            

    except Exception as exception:
            # change proxy if has proxy
            logging.error(exception)
            raise exception


if __name__ == "__main__":
  split_file('https://x-welp.azurewebsites.net/')