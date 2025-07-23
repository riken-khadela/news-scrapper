import requests
import urllib.parse
import datetime
import logging
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import time, random, os
import settings as cf
from bot import get_google_search_results, parse_google_results


  
def collect_page_details(sector, keywords, geo_locations = ['US']):
    logger = cf.logger('log/inc42.log')
    data = []
    for geo in geo_locations:
        search_term = f"{sector['sector']} + {keywords}"
        html = get_google_search_results(search_term, geo, 'd', logger)
        if html:
            data = parse_google_results(html, sector, keywords, search_term, "inc42.com", logger)
            time.sleep(2)
        else:
            print("No HTML returned for: %s", search_term)
    
    return data

