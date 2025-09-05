# logger.py

import os
import time, datetime
import base64, pytz, os, json, random, time
tz = pytz.timezone('Asia/Kolkata')

class CustomLogger:
    def __init__(self, log_file_path=''):
        if not log_file_path :
            print*"Please provide a valid logger path"
            return 
        
        self.log_file_path = os.path.join(os.getcwd(),log_file_path)
        self.setup_logger()

    def setup_logger(self):
        log_dir = os.path.dirname(self.log_file_path)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            print(f"Created directory: {log_dir}")
            
        if not os.path.exists(self.log_file_path):
            with open(self.log_file_path, 'a'): 
                pass
            print(f"Created log file: {self.log_file_path}")

    def log(self, message):
        timestamp = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"{timestamp} - INFO - {message}\n"
        with open(self.log_file_path, 'a') as log_file:
            log_file.write(log_message)

    def error(self, message):
        timestamp = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"{timestamp} - ERROR - {message}\n"
        with open(self.log_file_path, 'a') as log_file:
            log_file.write(log_message)

    def warning(self, message):
        timestamp = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"{timestamp} - WARNING - {message}\n"
        with open(self.log_file_path, 'a') as log_file:
            log_file.write(log_message)
