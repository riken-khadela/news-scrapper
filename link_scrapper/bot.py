from bs4 import BeautifulSoup
import datetime
import requests
from fake_useragent import UserAgent
import urllib.parse
import settings as cf
import logging, os
import glob
from settings import proxies, logger as get_logger  # assuming `proxies()` and `logger()` are in settings.py

def get_google_search_results(search_term, site_name, time='w', country_code='US'):
    logger = get_logger(site_name)  # dynamically get site-specific logger (combined log file)

    if time not in ['d', 'w', 'm', 'y']:
        raise ValueError("Invalid time parameter. Use 'd', 'w', 'm', or 'y'.")
    if not site_name or not search_term or not search_term.strip():
        raise ValueError("Site name and search term must be non-empty.")

    search_term = search_term.strip()

    try:
        headers = {"User-Agent": UserAgent().random}
        query = f"{search_term} site:{site_name}"
        params = {"q": query,"tf": f"pm",}
        # https://search.brave.com/search?q=Funding+%26+Investment+site%3Atechcrunch.com&source=desktop&tf=pd
        search_url = f"https://search.brave.com/search?{urllib.parse.urlencode(params)}"
        response = requests.get(search_url, headers=headers, timeout=10, proxies=proxies())
        
        logger.info("Response status code: %s", response.status_code)
        breakpoint()
        if response.status_code == 200:
            logger.info("Fetched results for: %s", search_term)

            # Save to numbered HTML file
            folder = os.path.join("html")
            os.makedirs(folder, exist_ok=True)
            site_prefix = site_name.split('.')[0]

            existing_files = glob.glob(os.path.join(folder, f"{site_prefix}*.html"))
            numbers = [
                int(os.path.splitext(os.path.basename(f))[0].replace(site_prefix, ""))
                for f in existing_files
                if os.path.splitext(os.path.basename(f))[0].replace(site_prefix, "").isdigit()
            ]
            next_index = max(numbers) + 1 if numbers else 1
            filename = os.path.join(folder, f"{site_prefix}{next_index}.html")

            with open("first.html", "w", encoding="utf-8") as f:f.write(response.text)
            breakpoint()
            logger.info("Saved HTML to %s", filename)
            return response.text

    except Exception as e:
        logger.error("Exception during Google Search fetch: %s", str(e))

    return None

def clean_google_url(href: str) -> str:
    if href.startswith("/url?q="):
        href = unquote(href.split("/url?q=")[-1].split("&")[0])
    return href

def parse_google_results(html, key_data, tag, search_term, site_name, logger ):
    soup = BeautifulSoup(html, 'lxml')
    extracted_links = []
    index = 0
    breakpoint()
    anchor_tags = soup.find_all("a")
    for a_tag in anchor_tags:
        if not a_tag.get("href"):
            continue
        href = clean_google_url(a_tag.get("href"))
        print("Processing URL: %s", href)
        if not href or site_name not in href:
            continue
        elif href == f"https://www.{site_name}/" or href == f"https://{site_name}/" or f"https://www.{site_name}/latest/" in href or f"https://{site_name}/latest/" in href:
            continue
        elif href.startswith("/url?q") or href.startswith("/search?"):
            continue
        elif "google.com" in href or "googleusercontent.com" in href:
            continue
            
        index += 1
        title_element = a_tag.find("h3")
        if not title_element :
            continue
        
        title = title_element.get_text(strip=True) if title_element else "No Title Found"

        obj = {
            "sector": key_data["sector"],
            "tag" : tag,
            "search_string": search_term,
            "url": href,
            "title": title,
            "pub_date": datetime.datetime.now().replace(hour=0, minute=0, second=0),
            "created_at": datetime.datetime.now(),
            "is_read": 0,
            "status": "pending",
            "index": index,
            "google_page": 1,
            "count": 1
        }

        extracted_links.append(obj)
        logger.info("Collected URL: %s", href)

    if not extracted_links:
        logger.warning("No valid links extracted for search: %s", search_term)

    return extracted_links