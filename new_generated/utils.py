"""
Shared utilities for AI-generated scrapers
"""

import json
import logging
import os
import time
import re
from datetime import datetime
from typing import Optional, Dict, Any
import requests
from fake_useragent import UserAgent
from pymongo import MongoClient
from new_generated.config import (
    MAX_RETRIES, RETRY_DELAY, LOG_DIR, LOG_FORMAT, LOG_DATE_FORMAT
)

CONFIG_FILE = "config.json"

def get_db_connection():
    """Get MongoDB connection using config.json"""
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)
    
    mongo_uri = config["mongo_db_connection"]
    return MongoClient(mongo_uri)

def setup_logger(name: str, log_file: str) -> logging.Logger:
    """
    Set up logger with file and console handlers
    
    Args:
        name: Logger name
        log_file: Log file name (will be created in LOG_DIR)
    
    Returns:
        Configured logger instance
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, log_file)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if logger.handlers:
        return logger
    
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def make_request(url: str, timeout: int = 10, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    """
    Make HTTP request with retry logic
    
    Args:
        url: URL to request
        timeout: Request timeout in seconds
        retries: Number of retry attempts
    
    Returns:
        Response object if successful, None otherwise
    """
    headers = {"User-Agent": UserAgent().random}
    
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response
            elif response.status_code == 404:
                return None
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
            else:
                return None
    
    return None

def clean_text(text: str) -> str:
    """
    Clean and normalize text content
    
    Args:
        text: Raw text to clean
    
    Returns:
        Cleaned text
    """
    if not text:
        return ""
    
    text = text.replace('\xa0', ' ')
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    return text.strip()

def word_count(text: str) -> int:
    """
    Count words in text
    
    Args:
        text: Text to count words in
    
    Returns:
        Number of words
    """
    if not text:
        return 0
    return len(text.split())

def parse_date(date_str: str) -> Optional[datetime]:
    """
    Parse date string to datetime object
    
    Args:
        date_str: Date string in various formats
    
    Returns:
        datetime object if parsing successful, None otherwise
    """
    date_formats = [
        '%Y-%m-%d',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%B %d, %Y',
        '%b %d, %Y',
        '%d %B %Y',
        '%d %b %Y'
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None

def smart_merge(existing_doc: Dict[str, Any], new_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge new document with existing, preserving fields that exist
    in the database but are missing from new scrape.
    
    Args:
        existing_doc: Existing document from database
        new_doc: Newly scraped document
    
    Returns:
        Merged document
    """
    merged = existing_doc.copy()
    
    for key, new_value in new_doc.items():
        if new_value is not None and new_value != "":
            merged[key] = new_value
    
    merged['last_updated'] = datetime.now()
    
    return merged

def extract_domain(url: str) -> str:
    """
    Extract domain from URL
    
    Args:
        url: Full URL
    
    Returns:
        Domain name
    """
    match = re.search(r'https?://(?:www\.)?([^/]+)', url)
    if match:
        return match.group(1)
    return ""
