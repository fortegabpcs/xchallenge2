
from datetime import datetime
import json
import logging
import time
import requests
import tenacity
from bs4 import BeautifulSoup

from urllib.parse import quote, urlparse
from urllib import parse

# Yelp Fusion no longer uses OAuth as of December 7, 2017.
# You no longer need to provide Client ID to fetch Data
# It now uses private keys to authenticate requests (API Key)
# You can find it on
# https://www.yelp.com/developers/v3/manage_app


# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.


#Defaults for our simple example.
DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'San Francisco, CA'
SEARCH_LIMIT = 50

class YelpSpider:

    def __init__(self):
        # get proxy
        pass

    """
        start_number 为偏移量，30的倍数，从0开始
    """

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(0.5))
    def request(self, host, path, api_key, url_params=None):
        url_params = url_params or {}
        url = '{0}{1}'.format(host, quote(path.encode('utf8')))
        headers = {
            'Authorization': 'Bearer %s' % api_key,
        }

        print(u'Querying {0} ...'.format(url))

        response = requests.request('GET', url, headers=headers, params=url_params)

        return response.json()
    

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(0.5))
    def search(self, api_key, term, location, offset):
        url_params = {
            'term': term.replace(' ', '+'),
            'location': location.replace(' ', '+'),
            'limit': SEARCH_LIMIT,
            'offset': offset
        }

        return self.request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)



    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(0.5))
    def get_business(self, api_key, business_id):
        business_path = BUSINESS_PATH + business_id

        return self.request(API_HOST, business_path, api_key)

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(0.5))
    def query_api(self, term, location, offset, API_KEY):
        response = self.search(API_KEY, term, location, offset)
        return response

    def get_stars(self, review):
        try:
            if review.find("div",{"class":"i-stars__373c0___sZu0 i-stars--regular-5__373c0__20dKs border-color--default__373c0__r305k overflow--hidden__373c0__3E2fM"}):
                return int(review.find("div",{"class":"i-stars__373c0___sZu0 i-stars--regular-5__373c0__20dKs border-color--default__373c0__r305k overflow--hidden__373c0__3E2fM"}).get('aria-label')[0])

            if review.find("div",{"class":"i-stars__373c0___sZu0 i-stars--regular-4__373c0__3b-zE border-color--default__373c0__r305k overflow--hidden__373c0__3E2fM"}):
                return int(review.find("div",{"class":"i-stars__373c0___sZu0 i-stars--regular-4__373c0__3b-zE border-color--default__373c0__r305k overflow--hidden__373c0__3E2fM"}).get('aria-label')[0])

            if review.find("div",{"class":"i-stars__373c0___sZu0 i-stars--regular-3__373c0__3XiEH border-color--default__373c0__r305k overflow--hidden__373c0__3E2fM"}):
                return int(review.find("div",{"class":"i-stars__373c0___sZu0 i-stars--regular-3__373c0__3XiEH border-color--default__373c0__r305k overflow--hidden__373c0__3E2fM"}).get('aria-label')[0])

            if review.find("div",{"class":"i-stars__373c0___sZu0 i-stars--regular-2__373c0__2yu75 border-color--default__373c0__r305k overflow--hidden__373c0__3E2fM"}):
                return int(review.find("div",{"class":"i-stars__373c0___sZu0 i-stars--regular-2__373c0__2yu75 border-color--default__373c0__r305k overflow--hidden__373c0__3E2fM"}).get('aria-label')[0])

            if review.find("div",{"class":"i-stars__373c0___sZu0 i-stars--regular-1__373c0__2QZgK border-color--default__373c0__r305k overflow--hidden__373c0__3E2fM"}):
                return int(review.find("div",{"class":"i-stars__373c0___sZu0 i-stars--regular-1__373c0__2QZgK border-color--default__373c0__r305k overflow--hidden__373c0__3E2fM"}).get('aria-label')[0])
            
            return None
        except Exception as exception:
            print(exception)
            raise exception

    def get_welp_stars(self, review):
        try:
            if review.find("p",{"class":"stars"}):
                return int(review.find("p",{"class":"stars"}).text[0])
            
            return None
        except Exception as exception:
            print(exception)
            raise exception


    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(1))
    def get_business_page(self, url, start_number):
        #url = url + '?start=' + str(start_number) if start_number > 0 else url
        try:
            logging.info("making request for business page {}".format(url))
            response = requests.get(url=url, timeout=10)
            return response

        except Exception as exception:
            logging.info("request failedclear {}".format(url))
            # change proxy if has proxy
            raise exception

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(1))
    def get_detail(self, url, start_number, page_text):

        #url = url + '?start=' + str(start_number) if start_number > 0 else url
        try:
            text = page_text

            soup = BeautifulSoup(text, "html")

            allrev = soup.find_all("div",{"class":"review"})

            u=[]
            l={}
            for i in range(0,len(allrev)):
                try:
                    l["review_url"]=allrev[i].find("a",{"class":"css-166la90"}).text
                except:
                    l["review_url"]=None
                try:
                    l["date"]=allrev[i].find("span",{"class":"css-e81eai"}).text
                except:
                    l["date"]=None
                try:
                    l["author_name"]=allrev[i].find("a",{"class":"css-166la90"}).text
                except:
                    l["author_name"]=None
                try:
                    l["location"]=allrev[i].find("span",{"class":"css-n6i4z7"}).text
                except:
                    l["location"]=None
                try:
                    l["rating"]= self.get_stars(allrev[i])
                except:
                    l["rating"]=None
                try:
                    l["review_text"]=allrev[i].find("span",{"class":"raw__373c0__tQAx6"}).text
                except:
                    l["review_text"]=None
                try:
                    l["author_avatar"]=allrev[i].find("img",{"class":"css-xlzvdl"})['src']
                except:
                    l["author_avatar"]=None
                try:
                    l["author_url"]=allrev[i].find("a",{"class":"css-5r1d0t"})['href']
                except:
                    l["author_url"]=None
                try:
                    l["lang_code"]=allrev[i].find("span",{"class":"raw__373c0__tQAx6"})['lang']
                except:
                    l["lang_code"]=None
                try:
                    l["meta_data"]={
                        'author_contributions': 0,
                        "feedback": {
                            "useful": 0,
                            "funny": 0,
                            "cool": 0
                        }
                    }
                except:
                    l["meta_data"]=None
                u.append(l)
                l={}
            #print({"data":u})

            return u

        except Exception as exception:
            # change proxy if has proxy
            raise exception


    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(0.5))
    def get_page(self, url):
        #url = url + '?start=' + str(start_number) if start_number > 0 else url
        try:
            logging.info("making request for business page {}".format(url))
            response = requests.get(url=url, timeout=200)
            return response

        except Exception as exception:
            logging.info("request failedclear {}".format(url))
            # change proxy if has proxy
            raise exception
    
    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(1))
    def get_welp_business_list(self, url, start_number, page_text, API_ENDPOINT):

        try:
            text = page_text

            soup = BeautifulSoup(text, "lxml")

            
            now = datetime.now()
            logging.info("started welp parsing... {}".format(now))

            allBiz = soup.find_all("div",{"class":"bizDiv"})

            u=[]
            l={}
            
            now = datetime.now()
            logging.info("parsing businesses 1 by 1... {}".format(now))
            
            for i in range(0,len(allBiz)):
                try:
                    biz_url = allBiz[i].find("a")['href']
                    biz_url = biz_url.replace('#', '%23')
                    parsed_url = urlparse(url)
                    #quoted_biz_url = parse.quote(biz_url, safe='/', encoding=None, errors=None)
                    parsed_biz_url = urlparse(biz_url)
                    item_url = parsed_url.scheme + "://" + parsed_url.netloc + biz_url

                    params = dict(parse.parse_qsl(parsed_biz_url.query))
                    # if(params):
                    #     with open('./downloads/params.json', 'w') as f:
                    #         f.write(json.loads(params))
                    #page_text = page.text
                    #logging.info(params)

                    item_address = params['address'] if 'address' in params else ''
                    item_address += '' if item_address == '' else ', '
                    item_address += params['city'] if 'city' in params else ''
                    item_address += (', ' + params['state']) if 'state' in params else ''
                    item_address += (', ' + params['zipcode']) if 'zipcode' in params else ''

                    msg = {
                        'id':params['biz'] if 'biz' in params else '',
                        'type':'welpreview',
                        #'term':term,
                        #'location':location,
                        'url': item_url,
                        'offset':0,
                        'review_count': params['review_count'] if 'review_count' in params else '',
                        "meta_data": {
                            "business_name": params['name'] if 'name' in params else '',
                            "business_address": item_address,
                            "business_categories": params['categories'] if 'categories' in params else '',
                            "total_reviews": params['review_count'] if 'review_count' in params else '',
                            "business_rating": 0, #params['rating'],
                            "total_reviews_per_language": [
                                {
                                    "code": "en",
                                    "count": params['review_count'] if 'review_count' in params else '',
                                }
                            ]
                        },
                        "total_reviews": params['review_count'] if 'review_count' in params else '',
                        "reviews":[],
                        "state": params['state'] if 'state' in params else ''
                    }
                    query = json.dumps(msg)
                    r = requests.post(url = API_ENDPOINT, json={"query": query})
                    logging.info('Post to url {} with status code {}'.format(r.url, r.status_code))
                    #u.append(msg)
                except Exception as exception:
                # change proxy if has proxy
                    logging.info("Message Exception: ")
                    logging.info(exception)
                    raise exception
            #print({"data":u})
            now = datetime.now()
            logging.info("finished parsing businesses 1 by 1... {}".format(now))

            return u

        except Exception as exception:
            # change proxy if has proxy
            raise exception


    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(1))
    def get_welp_detail(self, url, start_number, page_text):

        #url = url + '?start=' + str(start_number) if start_number > 0 else url
        try:
            text = page_text

            soup = BeautifulSoup(text, "html")

            allrev = soup.find_all("div",{"class":"review"})

            u=[]
            l={}
            for i in range(0,len(allrev)):
                try:
                    l["review_url"]=allrev[i].find("p",{"class":"stars"}).text
                except:
                    l["review_url"]=None
                try:
                    l["date"]=allrev[i].find("p",{"class":"date"}).text
                except:
                    l["date"]=None
                try:
                    l["author_name"]=allrev[i].find("a",{"class":"css-166la90"}).text
                except:
                    l["author_name"]=None
                try:
                    l["location"]=allrev[i].find("span",{"class":"css-n6i4z7"}).text
                except:
                    l["location"]=None
                try:
                    l["rating"]= self.get_welp_stars(allrev[i])
                except:
                    l["rating"]=None
                try:
                    l["review_text"]=allrev[i].find("p",{"class":"comment"}).text
                except:
                    l["review_text"]=None
                try:
                    l["author_avatar"]=allrev[i].find("img",{"class":"css-xlzvdl"})['src']
                except:
                    l["author_avatar"]=None
                try:
                    l["author_url"]=allrev[i].find("a",{"class":"css-5r1d0t"})['href']
                except:
                    l["author_url"]=None
                try:
                    l["lang_code"]=allrev[i].find("span",{"class":"raw__373c0__tQAx6"})['lang']
                except:
                    l["lang_code"]=None
                try:
                    l["meta_data"]={
                        'author_contributions': 0,
                        "feedback": {
                            "useful": 0,
                            "funny": 0,
                            "cool": 0
                        }
                    }
                except:
                    l["meta_data"]=None
                u.append(l)
                l={}
            #print({"data":u})

            return u

        except Exception as exception:
            # change proxy if has proxy
            raise exception
