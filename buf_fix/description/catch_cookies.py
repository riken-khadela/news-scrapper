import requests, json, urllib.parse, string, random, time, os
from datetime import datetime, timedelta
import threading
from logger import CustomLogger

CONFIG_FILE = "/media/rk/workspace/shushant/news-scrapper-1/buf_fix/description/config.json"
COOKIE_FILE = "/media/rk/workspace/shushant/news-scrapper-1/buf_fix/description/session_data.json"
CHECK_INTERVAL = 60  # seconds
REFRESH_INTERVAL = 25  # minutes

# Load config
with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

TOKEN = config["token_1"]
EMAIL = config["username"]
PASSWORD = config["password"]

# Threading locks
refresh_lock = threading.Lock()
refresh_event = threading.Event()
refresh_event.set() 

class CatchCookies:
    def __init__(self):
        self.logger = CustomLogger(log_file_path="log/CatchCookies.log")

    def generate_session_id(self):
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=7))

    def refresh_session(self):
        time.sleep(random.randint(1, 9))

        if not refresh_lock.locked():
            with refresh_lock:
                refresh_event.clear()
                try:
                    self.logger.log("Refreshing session...")
                    # --- Real refresh logic starts ---
                    session_id = self.generate_session_id()
                    jsonData = [
                        {"Action": "Fill", "Selector": "input[type='email']", "Value": EMAIL},
                        {"Action": "Fill", "Selector": "input[type='password']", "Value": PASSWORD},
                        {"Action": "Click", "Selector": "button[type='submit']"},
                        {"Action": "Wait", "Timeout": 5000}
                    ]
                    encodedJson = urllib.parse.quote_plus(json.dumps(jsonData))
                    login_url = (
                        f"http://api.scrape.do/?token={TOKEN}&url={urllib.parse.quote_plus('https://www.crunchbase.com/login')}"
                        f"&render=true&playWithBrowser={encodedJson}&sessionid={session_id}"
                    )

                    response = requests.get(login_url)
                    cookies = response.headers.get("Scrape.do-Cookies", "")
                    file_name = "catch_cookies.html"
                    with open(file_name, '+w') as f:
                        f.write(response.text)

                    if not os.path.exists(COOKIE_FILE):
                        raise FileNotFoundError("Cookie file not found.")

                    with open(COOKIE_FILE, "r") as f:
                        data = json.load(f)

                    data.update({
                        "session_id": session_id,
                        "cookies": cookies,
                        "last_refreshed": datetime.utcnow().isoformat(),
                        "updatting": False,
                        "status_update": True
                    })

                    with open(COOKIE_FILE, "w") as f:
                        json.dump(data, f, indent=2)

                    self.logger.log(f"[+] Session refreshed at {data['last_refreshed']} (Session ID: {session_id})")

                except Exception as e:
                    self.logger.error(f"[!] Error during session refresh: {e}")

                finally:
                    refresh_event.set() 
        else:
            refresh_event.wait()

    def needs_refresh(self):
        if not os.path.exists(COOKIE_FILE):
            return True
        try:
            with open(COOKIE_FILE, "r") as f:
                data = json.load(f)
            last_refreshed = data.get("last_refreshed")
            if not last_refreshed:
                return True
            last_time = datetime.fromisoformat(last_refreshed)
            return datetime.utcnow() - last_time > timedelta(minutes=REFRESH_INTERVAL)
        except Exception as e:
            self.logger.error(f"Error checking session refresh: {e}")
            return True

    def get_cookies(self):
        try:
            with open(COOKIE_FILE, "r") as f:
                data = json.load(f)
            return data.get("cookies")
        except Exception as e:
            self.logger.error(f"Failed to load cookies: {e}")
            return None
