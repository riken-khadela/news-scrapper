from update_scrapper.scrapper import MainUpdateScrapping1
from new_scrapper.scrapper import MainNewScrapping
import threading
import time
import traceback
from crunch_link_scrapper import collect_page_details

def run_MainUpdateScrapping1():
    scrapper = MainUpdateScrapping1()
    scrapper.thread_function()

def run_MainNewScrapping():
    scrapper = MainNewScrapping()
    scrapper.thread_function()

def run_cycle_for_4_hours():
    start_time = time.time()
    run_duration = 6 * 60 * 60  # 4 hours in seconds

    while time.time() - start_time < run_duration:
        try:
            collect_page_details()

            t1 = threading.Thread(target=run_MainUpdateScrapping1)
            t2 = threading.Thread(target=run_MainNewScrapping)

            t1.start()
            t2.start()

            t1.join()
            t2.join()

            print("[INFO] One iteration complete. Sleeping for 10 seconds before next iteration...")
            time.sleep(10)

        except Exception as e:
            print("[ERROR] Exception occurred:", e)
            traceback.print_exc()
            time.sleep(60)  # Wait 1 minute before retrying on error

if __name__ == "__main__":
    print("[START] Script initialized...")

    while True:
        print("[INFO] Starting 4-hour active cycle...")
        run_cycle_for_4_hours()

        print("[INFO] 4-hour cycle complete. Now sleeping for 4 hours...")
        time.sleep(2 * 60 * 60)  
