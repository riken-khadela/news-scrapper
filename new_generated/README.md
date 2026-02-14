# AI-Generated News Scraper

Clean, AI-generated scrapers for collecting news from specific sources using Brave Search and implementing smart data merging.

## Overview

This package provides two main components:

1. **Link Scraper** - Collects news URLs from Brave Search
2. **Details Scraper** - Scrapes article details with intelligent data merging

## Target Sources

- techcrunch.com
- inc42.com
- techfundingnews.com
- yourstory.com

## Database Structure

**Main Database:** `NEWSSCRAPERDATA`

**Collections:**
- `new_details_main_urls_only_temp` - Temporary storage for collected URLs
- `new_details` - Main collection for article details

## Usage

### Running the Link Scraper

Collects URLs from Brave Search for all target sites and keywords:

```bash
cd c:\Workspace\Shushant\main_news_scraper_git\news-scrapper
python -m AI_generated.link_scraper
```

This will:
- Search for articles using predefined keywords
- Collect URLs from all search result pages
- Skip duplicate URLs
- Store URLs in `new_details_main_urls_only_temp` collection
- **Process 8 site-keyword combinations concurrently for faster execution**

### Running the Details Scraper

Processes collected URLs and scrapes article details:

```bash
cd c:\Workspace\Shushant\main_news_scraper_git\news-scrapper
python -m AI_generated.details_scraper
```

This will:
- Fetch unprocessed URLs from temp collection
- Scrape article details using site-specific scrapers
- Filter out articles published before 2025-01-01
- Apply smart merging when updating existing articles
- Store/update articles in `new_details` collection
- **Process 10 URLs concurrently for faster scraping**

## Performance

Both scrapers use multi-threading for significantly improved performance:

- **Link Scraper**: Processes up to 8 site-keyword combinations simultaneously
- **Details Scraper**: Scrapes up to 10 articles simultaneously
- Typical speedup: 5-8x faster than sequential processing

## Smart Merging

The details scraper implements intelligent field-level merging:

- **Existing field with value** + **New scrape missing field** = **Preserve existing value**
- **Existing field with value** + **New scrape has value** = **Use new value**
- **Field doesn't exist** + **New scrape has value** = **Add new field**

This ensures no data loss during updates while keeping information current.

## Configuration

Edit `config.py` to customize:

- `TARGET_SITES` - Sites to scrape
- `SEARCH_KEYWORDS` - Keywords for Brave Search
- `DATE_CUTOFF` - Minimum publication date (default: 2025-01-01)
- `BATCH_SIZE` - Number of URLs to process per batch (default: 50)
- `MAX_LINK_SCRAPER_THREADS` - Concurrent threads for link scraper (default: 8)
- `MAX_DETAILS_SCRAPER_THREADS` - Concurrent threads for details scraper (default: 10)

## Logging

All operations are logged to the `log/` directory:

- `link_scraper.log` - Link collection activities
- `details_scraper.log` - Details scraping activities
- `<site>_scraper.log` - Site-specific scraping logs

## Project Structure

```
AI_generated/
├── __init__.py
├── config.py              # Configuration constants
├── utils.py               # Shared utilities
├── link_scraper.py        # URL collection from Brave Search
├── details_scraper.py     # Article details scraping
├── scrapers/
│   ├── __init__.py
│   ├── techcrunch.py      # TechCrunch scraper
│   ├── inc42.py           # Inc42 scraper
│   ├── techfundingnews.py # TechFundingNews scraper
│   └── yourstory.py       # YourStory scraper
└── README.md
```

## Requirements

- Python 3.7+
- pymongo
- beautifulsoup4
- lxml
- requests
- fake-useragent

## Notes

- Both scrapers use multi-threading for improved performance (8 threads for link scraper, 10 threads for details scraper)
- Brave Search doesn't support native date filtering, so articles are filtered by publication date during the details scraping phase
- The link scraper automatically handles pagination to collect all available results
- Duplicate URLs are automatically detected and skipped
- Failed scraping attempts are logged but don't stop the process
- Thread-safe operations ensure data integrity during concurrent processing
