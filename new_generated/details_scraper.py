"""
Details Scraper - Scrape article details from collected URLs

This module processes URLs from the temporary collection, scrapes article details
using site-specific scrapers, and stores/updates data in the main collection with
smart merging logic. Uses multi-threading for concurrent URL processing.
"""

from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from pymongo import UpdateOne, InsertOne
from pymongo.errors import BulkWriteError
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from new_generated.config import (
    DB_NAME_MAIN, COLLECTION_TEMP_URLS, COLLECTION_DETAILS,
    BATCH_SIZE, DATE_CUTOFF
)
from new_generated.utils import (
    get_db_connection, setup_logger, smart_merge, extract_domain
)
from new_generated.scrapers import (
    scrape_techcrunch, scrape_inc42, 
    scrape_techfundingnews, scrape_yourstory
)

logger = setup_logger('details_scraper', 'details_scraper.log')

SCRAPER_MAP = {
    "techcrunch.com": scrape_techcrunch,
    "inc42.com": scrape_inc42,
    "techfundingnews.com": scrape_techfundingnews,
    "yourstory.com": scrape_yourstory
}

def get_unprocessed_urls(batch_size: int = BATCH_SIZE):
    """
    Fetch unprocessed URLs from temporary collection
    
    Args:
        batch_size: Number of URLs to fetch
    
    Returns:
        List of URL documents
    """
    client = get_db_connection()
    db = client[DB_NAME_MAIN]
    collection = db[COLLECTION_TEMP_URLS]
    
    urls = list(collection.find({"processed": False}).limit(batch_size))
    
    client.close()
    
    logger.info(f"Fetched {len(urls)} unprocessed URLs")
    return urls

def mark_urls_as_processed(url_ids: list):
    """
    Mark URLs as processed in temporary collection
    
    Args:
        url_ids: List of document IDs to mark as processed
    """
    if not url_ids:
        return
    
    client = get_db_connection()
    db = client[DB_NAME_MAIN]
    collection = db[COLLECTION_TEMP_URLS]
    
    result = collection.update_many(
        {"_id": {"$in": url_ids}},
        {"$set": {"processed": True, "processed_at": datetime.now()}}
    )
    
    logger.info(f"Marked {result.modified_count} URLs as processed")
    
    client.close()

def scrape_article(url: str, site: str) -> Optional[Dict[str, Any]]:
    """
    Scrape article using appropriate site-specific scraper
    
    Args:
        url: Article URL
        site: Site domain
    
    Returns:
        Article data dictionary or None if scraping failed
    """
    scraper = SCRAPER_MAP.get(site)
    
    if not scraper:
        logger.warning(f"No scraper found for site: {site}")
        return None
    
    try:
        article_data = scraper(url)
        return article_data if article_data else None
    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return None

def is_article_recent(article_data: Dict[str, Any]) -> bool:
    """
    Check if article is published after the date cutoff
    
    Args:
        article_data: Article data dictionary
    
    Returns:
        True if article is recent, False otherwise
    """
    published_date = article_data.get('published_date')
    
    if not published_date:
        logger.warning(f"No published date for {article_data.get('url')}, allowing by default")
        return True
    
    if published_date >= DATE_CUTOFF:
        return True
    else:
        logger.info(f"Article too old ({published_date}), skipping: {article_data.get('url')}")
        return False

def save_articles_to_db(articles: list):
    """
    Save or update articles in main collection with smart merging
    
    Args:
        articles: List of article data dictionaries
    """
    if not articles:
        logger.info("No articles to save")
        return
    
    client = get_db_connection()
    db = client[DB_NAME_MAIN]
    collection = db[COLLECTION_DETAILS]
    
    collection.create_index("url", unique=True)
    
    bulk_operations = []
    insert_count = 0
    update_count = 0
    
    for article in articles:
        try:
            existing_doc = collection.find_one({"url": article["url"]})
            
            if not existing_doc:
                bulk_operations.append(InsertOne(article))
                insert_count += 1
            else:
                merged_doc = smart_merge(existing_doc, article)
                
                bulk_operations.append(UpdateOne(
                    {"url": article["url"]},
                    {"$set": merged_doc}
                ))
                update_count += 1
                
        except Exception as e:
            logger.error(f"Error processing article {article.get('url')}: {e}")
            continue
    
    if bulk_operations:
        try:
            result = collection.bulk_write(bulk_operations, ordered=False)
            logger.info(f"Inserted: {insert_count}, Updated: {update_count}")
        except BulkWriteError as e:
            logger.error(f"Bulk write error: {e.details}")
    else:
        logger.info("No operations to perform")
    
    client.close()

def process_single_url(url_doc: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Any]:
    """
    Process a single URL
    
    Args:
        url_doc: URL document from database
    
    Returns:
        Tuple of (article_data or None, url_doc_id)
    """
    url = url_doc['url']
    site = url_doc['site']
    
    logger.info(f"Processing: {url}")
    
    article_data = scrape_article(url, site)
    
    if article_data:
        if is_article_recent(article_data):
            logger.info(f"✓ Successfully scraped: {article_data.get('title', 'No title')[:50]}...")
            return (article_data, url_doc['_id'])
        else:
            logger.info(f"⊘ Skipped old article: {url}")
            return (None, url_doc['_id'])
    else:
        logger.warning(f"✗ Failed to scrape: {url}")
        return (None, url_doc['_id'])

def process_batch():
    """Process one batch of URLs with multi-threading"""
    urls = get_unprocessed_urls()
    
    if not urls:
        logger.info("No unprocessed URLs found")
        return False
    
    logger.info(f"Processing {len(urls)} URLs with {min(10, len(urls))} concurrent threads")
    
    articles = []
    processed_ids = []
    lock = threading.Lock()
    
    # Process URLs concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {
            executor.submit(process_single_url, url_doc): url_doc
            for url_doc in urls
        }
        
        for future in as_completed(future_to_url):
            url_doc = future_to_url[future]
            try:
                article_data, url_id = future.result()
                
                with lock:
                    if article_data:
                        articles.append(article_data)
                    processed_ids.append(url_id)
                    
            except Exception as e:
                logger.error(f"Exception processing {url_doc.get('url')}: {e}")
                with lock:
                    processed_ids.append(url_doc['_id'])
    
    logger.info(f"Scraped {len(articles)} articles out of {len(urls)} URLs")
    
    if articles:
        save_articles_to_db(articles)
    
    mark_urls_as_processed(processed_ids)
    
    return True

def main():
    """Main execution function with multi-threading"""
    logger.info("=" * 80)
    logger.info("Starting Details Scraper (Multi-threaded)")
    logger.info("=" * 80)
    
    total_batches = 0
    
    for _ in range(100):
        logger.info(f"\n{'=' * 80}")
        logger.info(f"Processing batch {total_batches + 1}")
        logger.info(f"{'=' * 80}")
        
        has_more = process_batch()
        
        if not has_more:
            logger.info("No more URLs to process")
            break
        
        total_batches += 1
    
    logger.info("\n" + "=" * 80)
    logger.info(f"Details Scraper Complete")
    logger.info(f"Total batches processed: {total_batches}")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()
