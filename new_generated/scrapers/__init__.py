"""
Site-specific scrapers for news sources
"""

from .techcrunch import scrape as scrape_techcrunch
from .inc42 import scrape as scrape_inc42
from .techfundingnews import scrape as scrape_techfundingnews
from .yourstory import scrape as scrape_yourstory

__all__ = [
    'scrape_techcrunch',
    'scrape_inc42',
    'scrape_techfundingnews',
    'scrape_yourstory'
]
