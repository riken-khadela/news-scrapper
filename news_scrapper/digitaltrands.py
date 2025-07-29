from bs4 import BeautifulSoup
from lxml import etree
import urllib, requests, logging, time, datetime
from datetime import date, timedelta
from fake_useragent import UserAgent
import re, os
import settings as cf

TOKEN = "50612111dbab405ca9c28aacbd4bf0e2dc7d7b4c269"

logger_file = os.path.join(os.getcwd(),'log','digital_trends.log')

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

            if res.status_code == 200:
                return True, res
            else :
                print("-"*20,"digital trends")
        except requests.Timeout:
            print("Request timed out. Retrying...")
        except requests.RequestException as e:
            print("Request failed: %s", e)

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
        
        if not author:
            try:
                author_element = data.find('span', class_='author-name')
                if not author_element:
                    author_element = data.find('a', class_='author-name')
                if not author_element:
                    author_element = data.find('div', class_='byline')
                if not author_element:
                    author_element = data.find('span', class_='by-author')
                if not author_element:
                    author_element = data.select_one('.article-author')
                if not author_element:
                    author_element = data.select_one('.post-author')
                if not author_element:
                    author_element = data.select_one('[rel="author"]')
                
                if author_element:
                    author = author_element.get_text().strip()
                    author = re.sub(r'^By\s+', '', author, flags=re.IGNORECASE)
                    author = re.sub(r'\s+', ' ', author)
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
    
    image_xpath = """//meta[contains(@property, 'og:image')]/@content | 
                     //meta[contains(@property, 'og:image:secure_url')]/@content | 
                     //meta[contains(@name, 'twitter:image')]/@content |
                     //meta[contains(@property, 'image')]/@content | 
                     //meta[contains(@name, 'image')]/@content | 
                     //*[contains(@class, 'featured-image')]/@src | 
                     //img[contains(@class, 'featured-image')]/@src |
                     //*[contains(@class, 'hero-image')]/@src |
                     //img[contains(@class, 'hero-image')]/@src |
                     //*[contains(@class, 'article-image')]/@src |
                     //img[contains(@class, 'article-image')]/@src |
                     //*[contains(@class, 'post-image')]/@src |
                     //img[contains(@class, 'post-image')]/@src |
                     //figure[contains(@class, 'featured')]//img/@src |
                     //div[contains(@class, 'article-hero')]//img/@src |
                     //image/@src"""
    
    if dom.xpath(image_xpath):
        images = dom.xpath(image_xpath)
        for i in images:
            if i and (i.endswith('.jpg') or i.endswith('.jpeg') or i.endswith('.png') or i.endswith('.webp')):
                if i.startswith('//'):
                    image = 'https:' + i
                elif i.startswith('/'):
                    image = 'https://www.digitaltrends.com' + i
                else:
                    image = i
                break
    return image

def clean_content(raw_content: str) -> str:
    content = raw_content.replace('\xa0', ' ')

    content = re.sub(r'Image:.*?\n?', '', content, flags=re.IGNORECASE)
    content = re.sub(r'Photo:.*?\n?', '', content, flags=re.IGNORECASE)
    content = re.sub(r'Credit:.*?\n?', '', content, flags=re.IGNORECASE)
    content = re.sub(r'Source:.*?\n?', '', content, flags=re.IGNORECASE)

    content = re.sub(r'Digital Trends.*?newsletter.*?\n+', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'Subscribe.*?Digital Trends.*?\n+', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'Follow.*?Digital Trends.*?\n+', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'Editors\' Recommendations.*?\n+', '', content, flags=re.DOTALL | re.IGNORECASE)
    
    content = re.sub(r'Share.*?\n?', '', content, flags=re.IGNORECASE)
    content = re.sub(r'Tweet.*?\n?', '', content, flags=re.IGNORECASE)
    
    content = re.sub(r'Comments.*?\n?', '', content, flags=re.IGNORECASE)
    content = re.sub(r'Leave a comment.*?\n?', '', content, flags=re.IGNORECASE)

    content = re.sub(r'\n{2,}', '\n\n', content)

    return content.strip()

def scrape(url):
    isdone, res = get_request(url)
    obj = {}
    if isdone:
        data = BeautifulSoup(res.text, 'html.parser')
        data_dict = {}
        
        heading = data.find('h1')
        
        if heading:
            data_dict['heading'] = heading.get_text().strip()
                
        BlogContentMain = data.find('article')
        if BlogContentMain:
            unwanted_selectors = [
                'aside', '.advertisement', '.ads', '.social-share', 
                '.newsletter-signup', '.related-articles', '.comments',
                '.author-bio', '.tags', '.categories', '.share-buttons',
                '.recommended', '.trending', '.popular'
            ]
            
            for selector in unwanted_selectors:
                for unwanted in BlogContentMain.select(selector):
                    unwanted.decompose()
            
            data_dict['content'] = clean_content(BlogContentMain.get_text())

        main_div = data.find('div',{'id' : 'h-maincontent'})
        image = ""
        if main_div:
            for  i in main_div.find_all('img') : 
                src_ = i.get('src')
                if src_ :
                    if src_.startswith('https://www.digitaltrends.com') :
                        image = src_

        try:
            author = get_author(res, data_dict)
            description = data_dict.get('content', '')
            if not image:
                image = get_image(res, data_dict) 
            desc_len = word_count(description)
            
            if description != '' and desc_len >= 150:
                obj = {
                    "url": str(res.url),
                    "title": data_dict.get('heading', ''),
                    "author": author,
                    "description": description,
                    "image": image,
                    "word_count": desc_len,
                    "insert_date": datetime.datetime.now(),
                    "source": "DigitalTrends"
                }
                
                print("Article scraped successfully!")
                print(f"Title: {obj['title']}")
                print(f"Author: {obj['author']}")
                print(f"image: {obj['image']}")
                print(f"Word Count: {obj['word_count']}")
                print(f"Content Preview: {obj['description'][:200]}...")
                
            else:
                print(f"Article too short or no content found. Word count: {desc_len}")
                
        except Exception as e:
            print("Error processing URL: %s", e)
            print(f"Error processing article: {e}")

        print("Scraping completed!")
        
    return obj
