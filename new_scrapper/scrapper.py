from new_scrapper.summery import SUMMARY
from new_scrapper.finance import FINANCIAL
from new_scrapper.news import NEWS 
import new_scrapper.settings as cf
import threading, os, json
from datetime import datetime
from new_scrapper.investment import INVESTMENT
from logger import CustomLogger
from catch_coockies import CatchCookies

import threading, os, time, random
from concurrent.futures import ThreadPoolExecutor, as_completed



class MainNewScrapping(SUMMARY, FINANCIAL, NEWS, INVESTMENT, cf.main_setting):
    def __init__(self, number_of_records = random.randint(20,30), number_of_threads = 2):
        super().__init__()
        self.number_of_records = number_of_records
        self.number_of_threads = number_of_threads
        self.session_manager = CatchCookies()
        
        self.logger = CustomLogger(log_file_path="log/new_scrapping1.log")

    def main(self, url, dict):
        org_detail = {}
        try :
            
            summary = self.summary_process_logic(url, dict)
            financialurl = summary[1]
            newsurl = summary[2]
            investmenturl = summary[3]

            financial = {}
            news = {}
            investment_section = {}

            if financialurl:
                financial = self.financial_process_logic(financialurl, dict)

            if investmenturl:
                investment_section = self.investment_process_logic(investmenturl, dict)

            if newsurl:
                news = self.news_process_logic(newsurl, dict)

            if summary[0] and 'organization_url' in summary[0]:
                org_detail.update(summary[0])
                if financial.get('financial'):
                    org_detail.update(financial)
                if investment_section.get('investment'):
                    org_detail.update(investment_section)
                if news.get('news'):
                    org_detail.update(news)
        except Exception as e: self.logger.error(f"Error in main processing logic : {e}")
        return org_detail

    def thread_logic(self, all_documents):
        alldetails = []
        update_urls = []
        dicts = all_documents

        def process_and_append(url, dict_data):
            try:
                details = self.main(url, dict_data)
                if 'summary' in details:
                    return details, dict_data
            except Exception as e:
                self.logger.error(f"[ERROR] Exception in thread: {e}")
                return None, None
            
        with ThreadPoolExecutor(max_workers=self.number_of_threads) as executor:
            future_to_data = {executor.submit(process_and_append, d['url'], d): d for d in dicts}
            for future in as_completed(future_to_data):
                try:
                    result, update_data = future.result()
                    if result and 'organization_url' in result:
                        alldetails.append(result)
                        update_urls.append(update_data)
                except Exception as e:
                    self.logger.error(f"[ERROR] Processing failed: {e}")
                    
        # process_and_append(dicts[0]['url'], dicts[0])
        alldetails = [details for details in alldetails if 'organization_url' in details]
        CONFIG_FILE = "config.json"
        with open(CONFIG_FILE, "r") as f: 
            config = json.load(f)
        
        if len(alldetails) > 0:
            config['new_added_count'] += len(alldetails)
            with open(CONFIG_FILE, "w") as f: 
                json.dump(config, f, ensure_ascii=False, indent=4)

            self.logger.log(f"Total new added : {config['new_added_count']}")
            try:
                self.insert_organisation_details(alldetails)
                self.update_read_stat_urls(update_urls)
                
            except cf.DuplicateKeyError as e:
                self.logger.error("Skipping duplicate records:", e)

    def thread_function(self):
        
        for _ in range(5):
            try:
                # self.session_manager.refresh_session()
                dicts = self.read_crunch_details_new(self.number_of_records)
                if not dicts:
                    for _ in range(10):
                        self.logger.log("No documents found, sleeping...")
                        time.sleep(60)
                        dicts = self.read_crunch_details_new(self.number_of_records)
                        if dicts:
                            break
                self.thread_logic(dicts)
                self.logger.log(f"Wating for a minitue restart the process of scrapping after completed scrapping a batch size of {self.number_of_records}")
                time.sleep(60)
            except Exception as e:
                self.logger.error(f"[ERROR] Exception in main thread loop: {e}")
