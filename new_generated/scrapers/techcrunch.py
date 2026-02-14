"""
TechCrunch article scraper
"""

from bs4 import BeautifulSoup
from lxml import etree
import re
from datetime import datetime
from typing import Optional, Dict, Any
from new_generated.utils import make_request, clean_text, word_count, setup_logger

logger = setup_logger('techcrunch_scraper', 'techcrunch_scraper.log')

def get_author(response) -> str:
    """Extract author from TechCrunch article"""
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
    except Exception as e:
        logger.warning(f"Error extracting author: {e}")
    
    return ""

def get_image(response) -> str:
    """Extract featured image from TechCrunch article"""
    try:
        data = BeautifulSoup(response.text, 'lxml')
        dom = etree.HTML(str(data))
        
        image_xpath = """//meta[contains(@property, 'og:image')]/@content | 
                        //meta[contains(@property, 'og:image:secure_url')]/@content | 
                        //meta[contains(@name, 'twitter:image')]/@content |
                        //meta[contains(@property, 'image')]/@content | 
                        //meta[contains(@name, 'image')]/@content | 
                        //*[contains(@class, 'featured-image')]/@src | 
                        //img[contains(@class, 'featured-image')]/@src"""
        
        if dom.xpath(image_xpath):
            images = dom.xpath(image_xpath)
            for img in images:
                if img and (img.endswith('.jpg') or img.endswith('.jpeg') or 
                           img.endswith('.png') or img.endswith('.webp')):
                    return img
    except Exception as e:
        logger.warning(f"Error extracting image: {e}")
    
    return ""

def get_published_date(response) -> Optional[datetime]:
    """Extract published date from TechCrunch article"""
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
    """Clean TechCrunch-specific content"""
    content = clean_text(raw_content)
    
    content = re.sub(r'Image Credits:.*\\n?', '', content)
    content = re.sub(r'Techcrunch event.*?REGISTER NOW\\n+', '', content, 
                    flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'TechCrunch has an AI-focused newsletter!.*', '', content, 
                    flags=re.DOTALL)
    content = re.sub(r'\\n{2,}', '\\n\\n', content)
    
    return content.strip()

def scrape(url: str) -> Dict[str, Any]:
    """
    Scrape TechCrunch article
    
    Args:
        url: Article URL
    
    Returns:
        Dictionary with article data or empty dict if scraping failed
    """
    logger.info(f"Scraping TechCrunch article: {url}")
    
    response = make_request(url)
    if not response:
        logger.warning(f"Failed to fetch URL: {url}")
        return {}
    
    try:
        data = BeautifulSoup(response.text, 'html.parser')
        
        main_blog_div = data.find('div', {
            "class": "seamless-scroll-container wp-block-techcrunch-seamless-scroll-container"
        })
        
        if not main_blog_div:
            logger.warning(f"Main content div not found for: {url}")
            return {}
        
        heading_div = main_blog_div.find('div', {
            'class': 'article-hero article-hero--image-and-text has-green-500-background-color wp-block-techcrunch-article-hero'
        })
        
        title = ""
        if heading_div:
            heading = heading_div.find('h1')
            if heading:
                title = heading.text.strip()
        
        blog_content_main = main_blog_div.find('main')
        description = ""
        
        if blog_content_main:
            blog_content = blog_content_main.find('div', {
                'class': 'entry-content wp-block-post-content is-layout-constrained wp-block-post-content-is-layout-constrained'
            })
            if blog_content:
                description = clean_content(blog_content.text)
        
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
                "source": "techcrunch.com",
                "word_count": desc_len,
                "insert_date": datetime.now()
            }
            logger.info(f"Successfully scraped TechCrunch article: {title}")
            return article_data
        else:
            logger.warning(f"Article too short or no content. Word count: {desc_len}")
            return {}
            
    except Exception as e:
        logger.error(f"Error scraping TechCrunch article {url}: {e}")
        return {}
