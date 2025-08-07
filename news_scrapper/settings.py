import logging
from fake_useragent import UserAgent
import os, random, requests, time
import requests, urllib3, urllib, json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CONFIG_FILE = "config.json"
with open(CONFIG_FILE, "r") as f: config = json.load(f)
TOKEN = config["token"]
print(f"TOKEN : {TOKEN}")
def logger(file_name):
    """Initialize logging to a file."""
    check_log_file(file_name)
    logging.basicConfig(
        filename=file_name,
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    return logging.getLogger()

def check_log_file(file):
    """Create log file if it doesn't exist, including any missing parent directories."""
    
    dir_path = os.path.dirname(file)
    
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    
    if not os.path.exists(file):
        with open(file, 'w') as f:
            pass 

    return file


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

def get_scrape_do_requests(url):
    c = 0
    headers = {"User-Agent": UserAgent().random}
    while c < 10:
        try:
            encoded_url = urllib.parse.quote_plus(url)
            url = f"https://api.scrape.do/?token={TOKEN}&url={encoded_url}"
            res = requests.get(url, headers=headers, verify=False)
            print(f'scrape do URL: {res.url} : status code --> {res.status_code}',)

            if res.status_code == 200:
                return True, res

        except requests.Timeout:
            print("Request timed out. Retrying...")
        except requests.RequestException as e:
            print("Request failed: %s", e)

        print("Scrape do Checking try again: %d", c)
        time.sleep(0.5)
        c += 1

    return False, False


def get_request(url):
    c = 0
    headers = {"User-Agent": UserAgent().random}
    while c < 10:
        try:

            res = requests.get(url, headers=headers, proxies=proxies(), verify=False)
            print(f' URL: {res.url} : status code --> {res.status_code}',)

            if res.status_code == 200:
                return True, res
            else :
                return get_scrape_do_requests(url)
            
        except requests.Timeout:
            print("Request timed out. Retrying...")
        except requests.RequestException as e:
            return get_scrape_do_requests(url)

        print("Checking try again: %d", c)
        time.sleep(0.5)
        c += 1

    return False, False
