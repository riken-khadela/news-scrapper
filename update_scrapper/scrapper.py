from update_scrapper.summery import SUMMARY
from update_scrapper.finance import FINANCIAL
from update_scrapper.news import NEWS
import update_scrapper.settings as cf
import threading, os, time, random, json
from datetime import datetime
from update_scrapper.investment import INVESTMENT
from logger import CustomLogger
from concurrent.futures import ThreadPoolExecutor, as_completed
from catch_coockies import CatchCookies
import fcntl
from pathlib import Path

class MainUpdateScrapping1(SUMMARY, FINANCIAL, NEWS, INVESTMENT, cf.main_setting):
    def __init__(self, number_of_records = random.randint(20,30), number_of_threads = 4):
        super().__init__()
        self.number_of_records = number_of_records
        self.number_of_threads = number_of_threads
        self.logger = CustomLogger(log_file_path="log/update1.log")
        self.session_manager = CatchCookies()
        self.config_lock = threading.Lock()

    def main(self, url, dict):
        try:
            summary = self.summary_process_logic(url, dict)
            
            # Bug Fix: Check if summary is valid and has required elements
            if not summary or not isinstance(summary, (list, tuple)) or len(summary) < 4:
                self.logger.error(f"Invalid summary structure for URL: {url}")
                return {}
                
            financialurl = summary[1] if len(summary) > 1 else None
            newsurl = summary[2] if len(summary) > 2 else None
            investmenturl = summary[3] if len(summary) > 3 else None

            financial = {}
            news = {}
            investment_section = {}

            if financialurl:
                try:
                    financial = self.financial_process_logic(financialurl, dict)
                except Exception as e:
                    self.logger.error(f"Financial processing failed for {financialurl}: {e}")

            if investmenturl:
                try:
                    investment_section = self.investment_process_logic(investmenturl, dict)
                except Exception as e:
                    self.logger.error(f"Investment processing failed for {investmenturl}: {e}")

            if newsurl:
                try:
                    news = self.news_process_logic(newsurl, dict)
                except Exception as e:
                    self.logger.error(f"News processing failed for {newsurl}: {e}")

            org_detail = {}
            if summary[0] and 'organization_url' in summary[0]:
                org_detail.update(summary[0])
                org_detail["is_updated"] = 1
                org_detail["update_timestamp"] = datetime.now()
                
                # Bug Fix: Check if data exists before updating
                if financial and financial.get('financial'):
                    org_detail.update(financial)
                if investment_section and investment_section.get('investment'):
                    org_detail.update(investment_section)
                if news and news.get('news'):
                    org_detail.update(news)

            return org_detail
            
        except Exception as e:
            self.logger.error(f"Error in main processing for {url}: {e}")
            return {}

    def safe_config_update(self, increment_count):
        """Thread-safe config file update"""
        CONFIG_FILE = "config.json"
        config_path = Path(CONFIG_FILE)
        
        with self.config_lock:
            try:
                # Read current config
                config = {"update_count": 0}
                if config_path.exists():
                    with open(CONFIG_FILE, "r") as f:
                        config = json.load(f)
                
                # Update count
                config['update_count'] = config.get('update_count', 0) + increment_count
                
                # Write to temporary file first, then replace
                temp_file = CONFIG_FILE + ".tmp"
                with open(temp_file, "w") as f:
                    json.dump(config, f, ensure_ascii=False, indent=4)
                
                # Atomic replace
                os.replace(temp_file, CONFIG_FILE)
                
                return config['update_count']
                
            except Exception as e:
                self.logger.error(f"Config update failed: {e}")
                # Clean up temp file if it exists
                temp_file = CONFIG_FILE + ".tmp"
                if os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)
                    except:
                        pass
                return 0

    def thread_logic(self, all_documents):
        alldetails = []
        failed_urls = []

        def process_and_append(url, dict_data):
            try:
                self.logger.log(f"Processing URL: {url}")
                details = self.main(url, dict_data)
                # Bug Fix: Check for valid details instead of just 'summary'
                if details and 'organization_url' in details:
                    return details, dict_data, None
                else:
                    # Bug Fix: Log when data is dropped
                    self.logger.warning(f"No valid data returned for URL: {url}")
                    return None, dict_data, f"No valid data for {url}"
                    
            except Exception as e:
                error_msg = f"Exception processing {url}: {e}"
                self.logger.error(error_msg)
                return None, dict_data, error_msg

        # Bug Fix: Better exception handling in ThreadPoolExecutor
        successful_results = []
        failed_results = []
        
        try:
            with ThreadPoolExecutor(max_workers=self.number_of_threads) as executor:
                # Submit all tasks
                future_to_data = {}
                for d in all_documents:
                    if 'organization_url' in d:
                        future = executor.submit(process_and_append, d['organization_url'], d)
                        future_to_data[future] = d
                    else:
                        self.logger.warning(f"Document missing organization_url: {d}")
                
                # Process completed tasks
                for future in as_completed(future_to_data):
                    original_data = future_to_data[future]
                    try:
                        result, update_data, error = future.result(timeout=300)  # 5 minute timeout
                        if result and 'organization_url' in result:
                            successful_results.append(result)
                        elif error:
                            failed_results.append((original_data, error))
                    except Exception as e:
                        error_msg = f"Future processing failed for {original_data.get('organization_url', 'unknown')}: {e}"
                        self.logger.error(error_msg)
                        failed_results.append((original_data, error_msg))

        except Exception as e:
            self.logger.error(f"ThreadPoolExecutor failed: {e}")
            return

        # Bug Fix: Only process valid results
        alldetails = [details for details in successful_results if details and 'organization_url' in details]
        
        # Log results
        self.logger.log(f"Successfully processed: {len(alldetails)} records")
        self.logger.log(f"Failed to process: {len(failed_results)} records")
        
        if failed_results:
            for failed_data, error in failed_results[:5]:  # Log first 5 failures
                self.logger.error(f"Failed: {failed_data.get('organization_url', 'unknown')} - {error}")

        if len(alldetails) > 0:
            try:
                # Update config safely
                total_count = self.safe_config_update(len(alldetails))
                self.logger.log(f"Total records processed in this batch: {len(alldetails)}")
                self.logger.log(f"Total cumulative count: {total_count}")
                
                # Bug Fix: Handle database update errors properly
                cf.update_crunch_detail(alldetails)
                self.logger.log("Database update completed successfully")
                
            except cf.DuplicateKeyError as e:
                self.logger.warning(f"Duplicate key error (expected): {e}")
            except Exception as e:
                self.logger.error(f"Database update failed: {e}")
        else:
            self.logger.log("No valid records to update in database")

    def ensure_valid_session(self):
        """Only refresh session if it's expired or invalid"""
        try:
            if self.session_manager.needs_refresh():
                self.logger.log("Session expired, refreshing...")
                self.session_manager.refresh_session()
                self.logger.log("Session refreshed successfully")
            else:
                self.logger.log("Session is still valid, skipping refresh")
        except Exception as e:
            self.logger.error(f"Session check/refresh failed: {e}")
            # Force refresh on error
            try:
                self.session_manager.refresh_session()
                self.logger.log("Forced session refresh completed")
            except Exception as refresh_error:
                self.logger.error(f"Forced session refresh failed: {refresh_error}")
                raise

    def thread_function(self):
        """Bug Fix: Improved main loop with proper error handling and exit conditions"""
        consecutive_failures = 0
        max_failures = 5
        batch_count = 0
        max_batches = 10  # Limit total batches to prevent infinite processing

        # Initial session check
        try:
            self.ensure_valid_session()
        except Exception as e:
            self.logger.error(f"Failed to establish valid session: {e}")
            return

        
        while consecutive_failures < max_failures and batch_count < max_batches:
            batch_count += 1
            try:
                self.logger.log(f"Starting batch {batch_count}")
                
                # Check session validity before each batch
                self.ensure_valid_session()
                
                # Try to get documents
                dicts = None
                retry_count = 0
                max_retries = 3
                
                while retry_count < max_retries and not dicts:
                    try:
                        dicts = self.read_crunch_details(self.number_of_records)
                        if dicts:
                            break
                    except Exception as e:
                        retry_count += 1
                        self.logger.error(f"Failed to read documents (attempt {retry_count}): {e}")
                        if retry_count < max_retries:
                            time.sleep(30)
                
                if not dicts:
                    self.logger.log("No documents found after retries, checking again...")
                    # Bug Fix: Limited wait time instead of indefinite loop
                    wait_attempts = 0
                    max_wait_attempts = 10
                    
                    while wait_attempts < max_wait_attempts:
                        self.logger.log(f"No documents found, waiting... (attempt {wait_attempts + 1}/{max_wait_attempts})")
                        time.sleep(60)
                        try:
                            dicts = self.read_crunch_details(self.number_of_records)
                            if dicts:
                                break
                        except Exception as e:
                            self.logger.error(f"Error checking for documents: {e}")
                        wait_attempts += 1
                    
                    if not dicts:
                        self.logger.log("No documents found after extended wait, exiting")
                        break
                
                # Process documents
                self.logger.log(f"Processing {len(dicts)} documents")
                self.thread_logic(dicts)
                consecutive_failures = 0  # Reset on success
                
                self.logger.log(f"Batch {batch_count} completed, waiting {60} seconds before next batch")
                time.sleep(60)
                
            except KeyboardInterrupt:
                self.logger.log("Received interrupt signal, shutting down gracefully")
                break
            except Exception as e:
                consecutive_failures += 1
                self.logger.error(f"Exception in batch {batch_count} (failure {consecutive_failures}/{max_failures}): {e}")
                
                if consecutive_failures < max_failures:
                    backoff_time = min(30 * consecutive_failures, 300)  # Exponential backoff, max 5 minutes
                    self.logger.log(f"Waiting {backoff_time} seconds before retry")
                    time.sleep(backoff_time)
                else:
                    self.logger.error("Max consecutive failures reached, stopping")
                    break

        self.logger.log(f"Processing completed. Total batches: {batch_count}, Final failure count: {consecutive_failures}")


# # Bug Fix: Remove global variables that could cause conflicts
# if __name__ == "__main__":
#     # Example usage with proper error handling
#     try:
#         scraper = MainUpdateScrapping1(number_of_records=25, number_of_threads=4)
#         scraper.thread_function()
#     except Exception as e:
#         print(f"Fatal error: {e}")