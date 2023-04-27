print('Loading General Module')
from datetime import datetime, timedelta,date
import pytz
import heapq as hq
import logging
import boto3
import json
from general_module import today_date,today_date

    
class S3LogHandler(logging.Handler):
    __instance = None
    
    @staticmethod
    def getInstance(prefix,main_module_name,sub_module_name, bucket_name='dlk-cloud-tier-10-preprocessed-ml-dev'):
        
        if S3LogHandler.__instance == None:
            S3LogHandler(prefix,main_module_name,sub_module_name,bucket_name)
            
        else: S3LogHandler.__instance.__initiate_sub_module_specific_variables(sub_module_name)
            
        
        return S3LogHandler.__instance
                         
    def __init__(self, prefix,main_module_name,sub_module_name,  bucket_name='dlk-cloud-tier-10-preprocessed-ml-dev'):
        if S3LogHandler.__instance != None:
            raise Exception("Logging Single exists already!")
        else:
            S3LogHandler.__instance = self

            # Set up the logger
            self.s3_client = boto3.client('s3')
            logging.Handler.__init__(self)
            self.bucket_name = bucket_name
            self.prefix = prefix
            self.formatter = logging.Formatter('%(levelname)s - %(name)s - %(message)s')
            self.today = today_date()
            self.main_module_name = main_module_name
            self.log_file = str()
            
            self.__initiate_sub_module_specific_variables(sub_module_name)
            
            
    def __initiate_sub_module_specific_variables(self,sub_module_name):
            if self.log_file == '':
                self.log_file = f"{self.today} - {now_time()} - {self.main_module_name} - {sub_module_name} \n"
                self.filename = f"{self.prefix}{self.today}/{self.main_module_name}.log"
                self.__put_the_log_file()
                
            else:
                self.__save_log_file()
            
        
        

    def __object_exists(self):
        try:
            return self.s3_client.head_object(Bucket=self.bucket_name, Key=self.filename)
        except :
            return True    


    def __put_the_log_file(self):
        if self.__object_exists() == True:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=self.filename, Body=self.log_file)
            self.log_file = str()

    def __save_log_file(self):
        current_log = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.filename)
                
        
        appended_log = current_log['Body'].read().decode() + "\n" + self.log_file
                
        self.s3_client.put_object(Bucket=self.bucket_name, Key=self.filename, Body=appended_log)
        self.log_file = str() 


    def emit(self, record):
        # Save the log message to S3 if it is at least as severe as the specified logging level
        log_entry = f"{self.today} {now_time()} - {self.format(record)}"
        if record.msg.startswith('[END]'):
            self.log_file = self.log_file + "\n" + log_entry + "\n"
            self.__save_log_file()
            
        if record.levelno >= self.level:            
            self.log_file = self.log_file + "\n" + log_entry
            if record.levelno >= 40:
                current_log = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.filename)
        
                appended_log = current_log['Body'].read().decode() + "\n" + self.log_file
            
                self.s3_client.put_object(Bucket=self.bucket_name, Key=self.filename, Body=appended_log)
                self.log_file = str()                
