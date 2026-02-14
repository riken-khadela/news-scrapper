# Quick Start Guide

## Installation

```bash
# Install missing dependency
pip install fake-useragent

# Verify all dependencies
pip install pymongo beautifulsoup4 lxml requests fake-useragent
```

## Usage

### 1. Collect URLs from Brave Search

```bash
cd c:\Workspace\Shushant\main_news_scraper_git\news-scrapper
python -m AI_generated.link_scraper
```

This will search for news articles and store URLs in `new_details_main_urls_only_temp` collection.

### 2. Scrape Article Details

```bash
python -m AI_generated.details_scraper
```

This will process collected URLs and store article details in `new_details` collection.

## Configuration

Edit `AI_generated/config.py` to customize:

- `TARGET_SITES` - Sites to scrape
- `SEARCH_KEYWORDS` - Keywords for search
- `DATE_CUTOFF` - Minimum publication date
- `BATCH_SIZE` - URLs per batch

## Database Collections

**Input:** `NEWSSCRAPERDATA.new_details_main_urls_only_temp`
- Stores collected URLs from Brave Search
- Fields: url, site, title, search_keyword, processed, timestamps

**Output:** `NEWSSCRAPERDATA.new_details`
- Stores scraped article details
- Fields: url, title, author, description, image, published_date, source, word_count, timestamps

## Logs

Check `log/` directory for detailed logs:
- `link_scraper.log` - URL collection
- `details_scraper.log` - Article scraping
- `<site>_scraper.log` - Site-specific logs

## Smart Merging

When updating existing articles:
- Preserves existing fields if new scrape doesn't have them
- Updates fields with fresh data when available
- Adds new fields from scrape

## Troubleshooting

**No URLs collected?**
- Check internet connection
- Verify Brave Search is accessible

**Scraping fails?**
- Check site-specific logs
- Website HTML structure may have changed

**Duplicate errors?**
- Normal - URLs are automatically skipped
