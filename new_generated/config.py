"""
Configuration constants for AI-generated scrapers
"""

import datetime

TARGET_SITES = [
    "techcrunch.com",
    "inc42.com",
    "techfundingnews.com",
    "yourstory.com"
]

SEARCH_KEYWORDS = [
    "startup funding",
    "venture capital",
    "tech investment",
    "startup news",
    "technology innovation",
    "product launch",
    "acquisition",
    "merger"
]

DATE_CUTOFF = datetime.datetime(2025, 1, 1)

BATCH_SIZE = 50

MAX_RETRIES = 3
RETRY_DELAY = 5

# Threading configuration
MAX_LINK_SCRAPER_THREADS = 8
MAX_DETAILS_SCRAPER_THREADS = 10

DB_NAME_MAIN = "NEWSSCRAPERDATA"
DB_NAME_SECONDARY = "NEW_NEWSSCRAPPERDATA"

COLLECTION_TEMP_URLS = "new_details_main_urls_only_temp"
COLLECTION_DETAILS = "new_details"

LOG_DIR = "log"
LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
