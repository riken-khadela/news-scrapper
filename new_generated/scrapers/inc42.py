"""
Inc42 article scraper
"""

from bs4 import BeautifulSoup
from lxml import etree
import re
from datetime import datetime
from typing import Optional, Dict, Any
from new_generated.utils import make_request, clean_text, word_count, setup_logger

logger = setup_logger('inc42_scraper', 'inc42_scraper.log')

def get_author(response) -> str:
    """Extract author from Inc42 article"""
    try:
        data = BeautifulSoup(response.text, 'html.parser')
        tree = etree.HTML(str(data))
        
        meta_names = ['citation_author', 'dc.creator', 'parsely-author', 
                      'DC.Contributor', 'author', 'authors-name', 
                      'article-author', 'og:authors', 'og:author']
        
        for meta_name in meta_names:
            meta_authors = tree.xpath(f"//meta[@*='{meta_name}']")
            if meta_authors:
                authors = [meta_author.get('content') for meta_author in meta_authors 
                          if meta_author.get('content')]
                if authors:
                    return ' | '.join(authors).replace(",", "")
        
        author_element = data.find('span', class_='c-byline__author-name')
        if not author_element:
            author_element = data.find('a', class_='c-byline__author-name')
        if not author_element:
            author_element = data.select_one('[data-testid="byline-author"]')
        if not author_element:
            author_element = data.find('div', class_='byline')
        
        if author_element:
            return author_element.get_text().strip()
            
    except Exception as e:
        logger.warning(f"Error extracting author: {e}")
    
    return ""

def get_image(response) -> str:
    """Extract featured image from Inc42 article"""
    try:
        data = BeautifulSoup(response.text, 'lxml')
        dom = etree.HTML(str(data))
        
        image_xpath = """//meta[contains(@property, 'og:image')]/@content | 
                        //meta[contains(@property, 'og:image:secure_url')]/@content | 
                        //meta[contains(@name, 'twitter:image')]/@content |
                        //meta[contains(@property, 'image')]/@content | 
                        //meta[contains(@name, 'image')]/@content | 
                        //*[contains(@class, 'featured-image')]/@src | 
                        //img[contains(@class, 'featured-image')]/@src |
                        //figure[contains(@class, 'e-image')]//img/@src |
                        //div[contains(@class, 'c-entry-hero')]//img/@src"""
        
        if dom.xpath(image_xpath):
            images = dom.xpath(image_xpath)
            for img in images:
                if img and (img.endswith('.jpg') or img.endswith('.jpeg') or 
                           img.endswith('.png') or img.endswith('.webp')):
                    if img.startswith('//'):
                        return 'https:' + img
                    elif img.startswith('/'):
                        return 'https://inc42.com' + img
                    else:
                        return img
    except Exception as e:
        logger.warning(f"Error extracting image: {e}")
    
    return ""

def get_published_date(response) -> Optional[datetime]:
    """Extract published date from Inc42 article"""
    try:
        data = BeautifulSoup(response.text, 'html.parser')
        tree = etree.HTML(str(data))
        
        date_metas = tree.xpath("//meta[@property='article:published_time']/@content | "
                               "//meta[@name='publishdate']/@content | "
                               "//time[@class='timestamp']/@datetime")
        
        if date_metas:
            date_str = date_metas[0]
            if 'T' in date_str:
                date_str = date_str.split('T')[0]
            return datetime.strptime(date_str, '%Y-%m-%d')
    except Exception as e:
        logger.warning(f"Error extracting date: {e}")
    
    return None

def clean_content(raw_content: str) -> str:
    """Clean Inc42-specific content"""
    content = clean_text(raw_content)
    
    content = re.sub(r'Image:.*?\\n?', '', content, flags=re.IGNORECASE)
    content = re.sub(r'Photo by.*?\\n?', '', content, flags=re.IGNORECASE)
    content = re.sub(r'Illustration by.*?\\n?', '', content, flags=re.IGNORECASE)
    content = re.sub(r'Sign up for.*?newsletter.*?\\n+', '', content, 
                    flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'Subscribe to.*?\\n+', '', content, 
                    flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'Share this story.*?\\n?', '', content, flags=re.IGNORECASE)
    content = re.sub(r'\\n{2,}', '\\n\\n', content)
    
    return content.strip()

def scrape(url: str) -> Dict[str, Any]:
    """
    Scrape Inc42 article
    
    Args:
        url: Article URL
    
    Returns:
        Dictionary with article data or empty dict if scraping failed
    """
    logger.info(f"Scraping Inc42 article: {url}")
    
    response = make_request(url)
    if not response:
        logger.warning(f"Failed to fetch URL: {url}")
        return {}
    
    try:
        data = BeautifulSoup(response.text, 'html.parser')
        
        main_blog_div = data.find('article')
        if not main_blog_div:
            main_blog_div = data.find('div', class_='c-entry-content')
        if not main_blog_div:
            main_blog_div = data.find('div', class_='l-wrapper')
        if not main_blog_div:
            main_blog_div = data.find('main')
        
        if not main_blog_div:
            logger.warning(f"Main content div not found for: {url}")
            return {}
        
        title = ""
        heading = main_blog_div.find('h1', class_='entry-title')
        if not heading:
            heading = main_blog_div.find('h1')
        if not heading:
            heading = main_blog_div.select_one('[data-testid="headline"] h1')
        
        if heading:
            title = heading.get_text().strip()
        
        description = ""
        blog_content_main = main_blog_div.find('div', class_='entry-content')
        
        if not blog_content_main:
            blog_content_divs = main_blog_div.find_all('div', 
                class_='duet--article--article-body-component')
            if blog_content_divs:
                description = " \\n".join([div.text for div in blog_content_divs])
        else:
            content_div = blog_content_main.find('div', {'class': 'single-post-content'})
            if content_div:
                description = content_div.text
        
        description = clean_content(description)
        
        author = get_author(response)
        image = get_image(response)
        published_date = get_published_date(response)
        desc_len = word_count(description)
        
        if description and desc_len >= 150:
            article_data = {
                "url": str(response.url),
                "title": title,
                "author": author,
                "description": description,
                "image": image,
                "published_date": published_date,
                "source": "inc42.com",
                "word_count": desc_len,
                "insert_date": datetime.now()
            }
            logger.info(f"Successfully scraped Inc42 article: {title}")
            return article_data
        else:
            logger.warning(f"Article too short or no content. Word count: {desc_len}")
            return {}
            
    except Exception as e:
        logger.error(f"Error scraping Inc42 article {url}: {e}")
        return {}
