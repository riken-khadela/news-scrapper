"""
Link Scraper - Collect news URLs from Brave Search

This module searches for news articles from target sites using Brave Search
and stores URLs in the temporary collection for later processing.
Uses multi-threading for concurrent site processing.
"""

import urllib.parse
from datetime import datetime
from typing import List, Dict, Any
from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
from pymongo import InsertOne
from pymongo.errors import BulkWriteError
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading, random

from new_generated.config import (
    TARGET_SITES, SEARCH_KEYWORDS, BATCH_SIZE, 
    DB_NAME_MAIN, COLLECTION_TEMP_URLS
)
from new_generated.utils import get_db_connection, setup_logger

logger = setup_logger('link_scraper', 'link_scraper.log')

def proxies():
    plist = [
        "37.48.118.90:13082",
        "83.149.70.159:13082"
    ]
    prx = random.choice(plist)
    return {
        'http': 'http://' + prx,
        'https': 'http://' + prx
    }

def get_brave_search_results(search_term: str, site_name: str) -> str:
    """
    Fetch search results from Brave Search
    
    Args:
        search_term: Search query
        site_name: Site to search within
    
    Returns:
        HTML content of search results page
    """
    for _ in range(20):
        try:
            headers = {"User-Agent": UserAgent().random}
            query = f"{search_term} site:{site_name}"
            params = {"q": query, "tf": "pm"}
            search_url = f"https://search.brave.com/search?{urllib.parse.urlencode(params)}"
            
            response = requests.get(search_url, headers=headers, timeout=10, proxies=proxies())
            
            if response.status_code == 200:
                logger.info(f"Fetched results for: {search_term} site:{site_name}")
                return response.text
            else:
                logger.warning(f"Failed to fetch results. Status: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Exception during Brave Search: {e}")
            
    return ""

def get_next_page_url(html: str) -> str:
    """
    Extract next page URL from search results
    
    Args:
        html: HTML content of current page
    
    Returns:
        URL of next page or empty string if no next page
    """
    try:
        soup = BeautifulSoup(html, 'lxml')
        pagination_div = soup.find('div', {'id': 'pagination'})
        
        if pagination_div:
            next_page = pagination_div.find('a')
            if next_page:
                next_page_path = next_page.get('href')
                if next_page_path and "offset=0" not in next_page_path:
                    full_url = urllib.parse.urljoin("https://search.brave.com", next_page_path)
                    logger.info(f"Found next page: {full_url}")
                    return full_url
    except Exception as e:
        logger.error(f"Error extracting next page URL: {e}")
    
    return ""

def extract_urls_from_page(html: str, site_name: str, search_keyword: str) -> List[Dict[str, Any]]:
    """
    Extract article URLs from search results page
    
    Args:
        html: HTML content of search results
        site_name: Site being searched
        search_keyword: Keyword used for search
    
    Returns:
        List of URL documents
    """
    extracted_urls = []
    
    try:
        soup = BeautifulSoup(html, 'lxml')
        result_div = soup.find('div', {'id': 'results'})
        
        if not result_div:
            logger.warning("No results div found in HTML")
            return extracted_urls
        
        for result in result_div.find_all('div', {'class': 'snippet'}):
            try:
                link = result.find('a')
                title_div = result.find('div', {'class': 'title'})
                
                if not link or not title_div:
                    continue
                
                url = link.get('href')
                title = title_div.text.strip()
                
                if not url or site_name not in url:
                    continue
                
                if url in [f"https://www.{site_name}/", f"https://{site_name}/"]:
                    continue
                
                if "/latest/" in url or url.startswith("/url?q") or url.startswith("/search?q"):
                    continue
                
                url_doc = {
                    "url": url,
                    "site": site_name,
                    "title": title,
                    "search_keyword": search_keyword,
                    "collected_at": datetime.now(),
                    "processed": False,
                    "created_at": datetime.now()
                }
                
                extracted_urls.append(url_doc)
                logger.info(f"Collected URL: {url}")
                
            except Exception as e:
                logger.error(f"Error processing search result: {e}")
                continue
        
    except Exception as e:
        logger.error(f"Error extracting URLs from page: {e}")
    
    return extracted_urls

def fetch_all_pages(search_term: str, site_name: str, search_keyword: str) -> List[Dict[str, Any]]:
    """
    Fetch URLs from all pages of search results
    
    Args:
        search_term: Search query
        site_name: Site to search
        search_keyword: Original keyword
    
    Returns:
        List of all URL documents from all pages
    """
    all_urls = []
    
    html = get_brave_search_results(search_term, site_name)
    if not html:
        return all_urls
    
    page_count = 1
    
    while html:
        logger.info(f"Processing page {page_count} for {site_name}")
        
        urls = extract_urls_from_page(html, site_name, search_keyword)
        all_urls.extend(urls)
        
        next_page_url = get_next_page_url(html)
        if not next_page_url:
            logger.info("No more pages to process")
            break
        
        try:
            headers = {"User-Agent": UserAgent().random}
            response = requests.get(next_page_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                html = response.text
                page_count += 1
            else:
                logger.warning(f"Failed to fetch next page. Status: {response.status_code}")
                break
                
        except Exception as e:
            logger.error(f"Error fetching next page: {e}")
            break
    
    logger.info(f"Total URLs collected for {site_name}: {len(all_urls)}")
    return all_urls

def insert_urls_to_db(urls: List[Dict[str, Any]]):
    """
    Insert URLs to temporary collection, skipping duplicates
    
    Args:
        urls: List of URL documents to insert
    """
    if not urls:
        logger.info("No URLs to insert")
        return
    
    client = get_db_connection()
    db = client[DB_NAME_MAIN]
    collection = db[COLLECTION_TEMP_URLS]
    
    collection.create_index("url", unique=True)
    
    bulk_operations = []
    
    for url_doc in urls:
        existing = collection.find_one({"url": url_doc["url"]})
        
        if not existing:
            bulk_operations.append(InsertOne(url_doc))
        else:
            logger.info(f"Duplicate URL skipped: {url_doc['url']}")
    
    if bulk_operations:
        try:
            result = collection.bulk_write(bulk_operations, ordered=False)
            logger.info(f"Inserted {result.inserted_count} new URLs")
        except BulkWriteError as e:
            logger.error(f"Bulk write error: {e.details}")
    else:
        logger.info("No new URLs to insert (all duplicates)")
    
    client.close()

def process_site_keyword(site: str, keyword: str) -> int:
    """
    Process a single site-keyword combination
    
    Args:
        site: Site to search
        keyword: Keyword to search for
    
    Returns:
        Number of URLs collected
    """
    logger.info(f"[{site}] Searching for: {keyword}")
    
    search_term = f"{keyword}"
    urls = fetch_all_pages(search_term, site, keyword)
    
    if urls:
        insert_urls_to_db(urls)
        logger.info(f"[{site}] Collected {len(urls)} URLs for keyword: {keyword}")
        return len(urls)
    
    return 0

def main():
    """Main execution function with multi-threading"""
    logger.info("=" * 80)
    logger.info("Starting Link Scraper (Multi-threaded)")
    logger.info("=" * 80)
    
    total_urls_collected = 0
    lock = threading.Lock()
    
    # Create list of all site-keyword combinations
    tasks = []
    for site in TARGET_SITES:
        for keyword in SEARCH_KEYWORDS:
            tasks.append((site, keyword))
    
    logger.info(f"Total tasks to process: {len(tasks)}")
    logger.info(f"Using {min(8, len(tasks))} concurrent threads")
    
    # Process tasks concurrently
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_task = {
            executor.submit(process_site_keyword, site, keyword): (site, keyword)
            for site, keyword in tasks
        }
        
        for future in as_completed(future_to_task):
            site, keyword = future_to_task[future]
            try:
                urls_count = future.result()
                with lock:
                    total_urls_collected += urls_count
                logger.info(f"✓ Completed: {site} - {keyword} ({urls_count} URLs)")
            except Exception as e:
                logger.error(f"✗ Failed: {site} - {keyword}: {e}")
    
    logger.info("\n" + "=" * 80)
    logger.info(f"Link Scraper Complete")
    logger.info(f"Total URLs collected: {total_urls_collected}")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()
