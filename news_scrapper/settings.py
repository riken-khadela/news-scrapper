import logging
import os, random

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
    plist = [
        "37.48.118.90:13082",
        "83.149.70.159:13082"
    ]
    prx = random.choice(plist)
    return {
        'http': 'http://' + prx,
        'https': 'http://' + prx
    }