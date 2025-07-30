import logging
from fake_useragent import UserAgent
import os, random, requests, time
import requests, urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TOKEN = "50612111dbab405ca9c28aacbd4bf0e2dc7d7b4c269"

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
    # plist = [
    #     "37.48.118.90:13082",
    #     "83.149.70.159:13082"
    # ]
    # prx = random.choice(plist)
    # return {
    #     'http': 'http://' + prx,
    #     'https': 'http://' + prx
    # }
    return {
    "http": f"http://brd-customer-hl_df6beaf5-zone-residential_proxy1:ay4093z76g9j@brd.superproxy.io:33335",
    "https": f"http://brd-customer-hl_df6beaf5-zone-residential_proxy1:ay4093z76g9j@brd.superproxy.io:33335",
}



def get_request(url):
    c = 0
    headers = {"User-Agent": UserAgent().random}
    while c < 10:
        try:

            res = requests.get(url, headers=headers, proxies=proxies(), verify=False)
            print(' URL: %s', res.url)

            if res.status_code == 200:
                return True, res
            else :
                res = requests.get(url)
                if res.status_code == 200:
                    return True, res
        except requests.Timeout:
            print("Request timed out. Retrying...")
        except requests.RequestException as e:
            print("Request failed: %s", e)

        print("Checking try again: %d", c)
        time.sleep(0.5)
        c += 1

    return False, False
