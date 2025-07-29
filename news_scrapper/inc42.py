from bs4 import BeautifulSoup
from lxml import etree
import urllib, requests, logging, time, datetime
from datetime import date, timedelta
from fake_useragent import UserAgent
import re, os
import settings as cf

TOKEN = "50612111dbab405ca9c28aacbd4bf0e2dc7d7b4c269"

logger_file = os.path.join(os.getcwd(),'log','inc42.log')

# logging.basicConfig(
#     filename=cf.check_log_file(logger_file),
#     level=print,
#     format='%(asctime)s [%(levelname)s] %(message)s'
# )
def get_request(url):
    c = 0
    print('Searching for: %s', url)

    while c < 10:
        try:
            
            
            res = requests.get(url, proxies=cf.proxies())
            print('URL: %s', res.url)
            print('*' * 100)
            
            print(res.status_code)
            if res.status_code == 200:
                return True, res
            else :
                print("-"*20,"inc42")
        except requests.Timeout:
            print("Request timed out. Retrying...",f": {res.status_code}")
        except requests.RequestException as e:
            print("Request failed: %s", e,)
            

        print("Checking try again: %d", c)
        time.sleep(0.5)
        c += 1

    return False, False

def get_author(response, my_dict):
    try:
        authors_data = []
        author = ''
        data = BeautifulSoup(response.text, 'html.parser')
        tree = etree.HTML(str(data))
        
        # The Verge specific meta tags and common ones
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
        
        # If no meta tags found, try The Verge specific author selectors
        if not author:
            try:
                # The Verge author patterns
                author_element = data.find('span', class_='c-byline__author-name')
                if not author_element:
                    author_element = data.find('a', class_='c-byline__author-name')
                if not author_element:
                    author_element = data.select_one('[data-testid="byline-author"]')
                if not author_element:
                    author_element = data.find('div', class_='byline')
                
                if author_element:
                    author = author_element.get_text().strip()
            except:
                pass
                
    except:
        pass
    return author

def word_count(text):
    return len(text.split())

def get_image(response, my_dict):
    image = ''
    data = BeautifulSoup(response.text, 'lxml')
    dom = etree.HTML(str(data))
    
    # The Verge specific image xpath with common ones
    image_xpath = """//meta[contains(@property, 'og:image')]/@content | 
                     //meta[contains(@property, 'og:image:secure_url')]/@content | 
                     //meta[contains(@name, 'twitter:image')]/@content |
                     //meta[contains(@property, 'image')]/@content | 
                     //meta[contains(@name, 'image')]/@content | 
                     //*[contains(@class, 'featured-image')]/@src | 
                     //img[contains(@class, 'featured-image')]/@src | 
                     //figure[contains(@class, 'e-image')]//img/@src |
                     //div[contains(@class, 'c-entry-hero')]//img/@src |
                     //image/@src"""
    
    if dom.xpath(image_xpath):
        images = dom.xpath(image_xpath)
        for i in images:
            if i and (i.endswith('.jpg') or i.endswith('.jpeg') or i.endswith('.png') or i.endswith('.webp')):
                # Handle relative URLs for The Verge
                if i.startswith('//'):
                    image = 'https:' + i
                elif i.startswith('/'):
                    image = 'https://inc42.com' + i
                else:
                    image = i
                break
    return image

def clean_content(raw_content: str) -> str:
    # Remove non-breaking spaces and replace with space
    content = raw_content.replace('\xa0', ' ')

    # Remove image credits - The Verge specific
    content = re.sub(r'Image:.*?\n?', '', content, flags=re.IGNORECASE)
    content = re.sub(r'Photo by.*?\n?', '', content, flags=re.IGNORECASE)
    content = re.sub(r'Illustration by.*?\n?', '', content, flags=re.IGNORECASE)

    # Remove The Verge promotional content
    content = re.sub(r'Sign up for.*?newsletter.*?\n+', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'Subscribe to.*?\n+', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'Share this story.*?\n?', '', content, flags=re.IGNORECASE)

    # Remove excessive newlines (more than 2 in a row)
    content = re.sub(r'\n{2,}', '\n\n', content)

    # Strip leading/trailing whitespace
    return content.strip()

def scrape(url) :

    # Main scraping logic - adapted for The Verge
    isdone, res = get_request(url)
    obj = {}
    if isdone:
        data = BeautifulSoup(res.text, 'html.parser')
        data_dict = {}
        
        # Find main article container - The Verge uses 'article' tag or specific classes
        main_blog_div = data.find('article')
        if not main_blog_div:
            main_blog_div = data.find('div', class_='c-entry-content')
        if not main_blog_div:
            main_blog_div = data.find('div', class_='l-wrapper')
        if not main_blog_div:
            main_blog_div = data.find('main')
        
        if main_blog_div:
            # Extract heading - The Verge patterns
            heading = main_blog_div.find('h1', class_='entry-title')
            if not heading:
                heading = main_blog_div.find('h1')
            if not heading:
                heading = main_blog_div.select_one('[data-testid="headline"] h1')
            
            if heading:
                data_dict['heading'] = heading.get_text().strip()
                    
            # Extract blog content - The Verge specific selectors
            BlogContentMain = main_blog_div.find('div', class_='entry-content')
            description = ""
            if not BlogContentMain:
                BlogContentMain = main_blog_div.find_all('div', class_='duet--article--article-body-component')
                
            
            if BlogContentMain:
                content_div = BlogContentMain.find('div',{'class':'single-post-content'})
                
                for i in main_blog_div.find_all('div', class_='duet--article--article-body-component') : 
                    description += " \n" + i.text
                
                data_dict['content'] = clean_content(content_div.text)
                if images:= content_div.find_all('img'):
                    data_dict['images'] = []
                    for img in images :
                        data_dict['images'].append(img.get('src'))
                        
        # Process the extracted data
        try:
            author = get_author(res, data_dict)
            description = data_dict.get('content', '')
            image = get_image(res, data_dict)
            desc_len = word_count(description)
            print(f"Description Length: {description}")
            if description != '' and desc_len >= 150:
                obj = {
                    "url": str(res.url),
                    "title": data_dict.get('heading', ''),
                    "author": author,
                    "description": description,
                    "image": image,
                    "word_count": desc_len,
                    "insert_date": datetime.datetime.now(),
                    "source": "The Verge"
                }
                
                print("Article scraped successfully!")
                print(f"Title: {obj['title']}")
                print(f"Author: {obj['author']}")
                print(f"Word Count: {obj['word_count']}")
                print(f"Content Preview: {obj['description'][:200]}...")
                
            else:
                print(f"Article too short or no content found. Word count: {desc_len}")
                
        except Exception as e:
            print("Error processing URL: %s", e)
            print(f"Error processing article: {e}")

    return obj

