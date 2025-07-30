from bs4 import BeautifulSoup
from lxml import etree
import urllib, requests, logging, time, datetime
from datetime import date, timedelta
from fake_useragent import UserAgent
import random, os
import settings as cf

TOKEN = "50612111dbab405ca9c28aacbd4bf0e2dc7d7b4c269"
logger_file = os.path.join(os.getcwd(),'log','tech_crunch.log')

# logging.basicConfig(
#     filename=cf.check_log_file(logger_file),
#     level=print,
#     format='%(asctime)s [%(levelname)s] %(message)s'
# )


import re

def get_author(response, my_dict):
        try:
            authors_data = []
            author = ''
            data = BeautifulSoup(response.text, 'html.parser')
            tree = etree.HTML(str(data))
            meta_names = ['citation_author', 'dc.creator', 'parsely-author', 'DC.Contributor', 'author',
                          'authors-name', 'article-author', 'og:authors', 'og:author']
            try:
                for meta_name in meta_names:
                    meta_authors = tree.xpath(f"//meta[@*='{meta_name}']")
                    if meta_authors:
                        authors = [meta_author.get('content') for meta_author in meta_authors if
                                   meta_author.get('content')]
                        if authors:
                            author = ' | '.join(authors).replace(",", "")
                            break
            except:
                pass
        except:
            pass
        return author

def word_count( text):
        return len(text.split())

def get_image(response, my_dict):
        image = ''
        data = BeautifulSoup(response.text, 'lxml')
        dom = etree.HTML(str(data))
        image_xpath = "//meta[contains(@property, 'image')]/@content | //meta[contains(@name, 'image')]/@content | //*[contains(@class, 'featured-image')]/@src | //img[contains(@class, 'featured-image')]/@src | //meta[contains(@property, 'og:image:secure_url')]/@content | //meta[contains(@property, 'og:image')]/@content | //image/@src"
        if dom.xpath(image_xpath):
            images = dom.xpath(image_xpath)
            for i in images:
                if i.endswith('.jpg') or i.endswith('.jpeg') or i.endswith('.png'):
                    image = i
                    break
        return image


def clean_content(raw_content: str) -> str:
    content = raw_content.replace('\xa0', ' ')
    content = re.sub(r'Image Credits:.*\n?', '', content)
    content = re.sub(r'Techcrunch event.*?REGISTER NOW\n+', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'TechCrunch has an AI-focused newsletter!.*', '', content, flags=re.DOTALL)
    content = re.sub(r'\n{2,}', '\n\n', content)
    return content.strip()


def scrape(url):
    obj = {}
    isdone, res  = cf.get_request(url)
    if isdone :
        data = BeautifulSoup(res.text, 'html.parser')
        data_dict = {}
        main_blog_div = data.find('div',{"class" : "seamless-scroll-container wp-block-techcrunch-seamless-scroll-container"})
        if main_blog_div :
            Heading_div = main_blog_div.find('div',{'class' : 'article-hero article-hero--image-and-text has-green-500-background-color wp-block-techcrunch-article-hero'})
            if Heading_div :
                heading = Heading_div.find_all('h1')
                if heading :
                    data_dict['heading'] = heading[0].text
                    
            # blog data
            BlogContentMain = main_blog_div.find('main')
            if BlogContentMain :
                blog_content = BlogContentMain.find('div',{'class' : 'entry-content wp-block-post-content is-layout-constrained wp-block-post-content-is-layout-constrained'})
                if blog_content:
                    data_dict['content'] = clean_content(blog_content.text)

        try:
            author = get_author(res, data_dict)
            description = data_dict.get('content','')
            image = get_image(res, data_dict)
            desc_len = word_count(description)
            if description != '' and desc_len >= 150:
                obj = {
                    "url": str(res.url),
                    "title": data_dict['heading'],
                    "author": author,
                    "description": description,
                    "image": image,
                    'insert_date': datetime.datetime.now()
                }
        except Exception as e:
            print("Error processing URL: %s", e)
            pass
        
    
    return obj
