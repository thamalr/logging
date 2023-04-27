print('Loading General Module')
from datetime import datetime, timedelta,date
import pytz
import heapq as hq
import snowflake_module
import logging
import boto3
import json

def today_date():
    """
    Input : No input
    
    Objective : Get current date.
    
    Output : current date
    """
    srilanka_tz = pytz.timezone("Asia/Colombo")
    date = datetime.now(srilanka_tz).strftime("%Y-%m-%d")
    return date

def exe_id():
    """
    Input : No input
    
    Objective : Returns current timestamp as execution id. (Ex : `20230220T085700`)
    
    Output : current timestamp       
    """
    srilanka_tz = pytz.timezone("Asia/Colombo")
    exe_id = datetime.now(srilanka_tz).strftime("%Y%m%dT%H%M%S")
    return exe_id

def now_time():
    """
    Input : No input
    
    Objective : Returns current time.
    
    Output : current time   
    """
    srilanka_tz = pytz.timezone("Asia/Colombo")
    now_time = datetime.now(srilanka_tz).strftime("%T%H%M%S")
    return now_time

def load_ts():
    """
    Input : No input
    
    Objective : Returns current date.
    
    Output : current date
    """
    srilanka_tz = pytz.timezone("Asia/Colombo")
    load_ts = datetime.now(srilanka_tz).strftime("%Y-%m-%d")
    return load_ts

def convert_to_heap(offer_list_dict):
    """
    Input : 
        offer_list_dict : list of offers
    
    Objective : Converts offer list into a priority queue(heap).
    
    Output : No output
    
    """
    lst_temp = list(offer_list_dict.items())
    hq.heapify(lst_temp)

def week_of_month():
    """
    Input : No input
    
    Objective : Returns the week corresponding to the current date.
    
    Output : current week
    """
    return f'WK{(datetime.today().day - 1) // 7 + 1}'

def day_of_week():
    """
    Input : No input
    
    Objective : Returns day of week corresponding to the current date.
    
    Output : current day of week    
    """
    return datetime.today().strftime('%A')

def create_snowflake_connection(secret_name,region):
    """
    Input : 
        secret_name : Name of the secret key
        region_name : Name of region
    
    Objective : Creates the snowflake connection and returns the object.
    
    Output : snowflake connection object   
    """
    snowflake_secret = {}
    snowflake_secret['secret_name'] = secret_name
    snowflake_secret['region'] = region
    print("[LOG] Region name as {}".format(snowflake_secret['region']))

    # Connecting to Snowflake using the default authenticator
    try:
        snowflake_context = snowflake_module.createSnowflakeConnection(snowflake_secret)
        cs=snowflake_context.cursor()
    except:
        print("Unable to Connect to Snowflake")
        raise

    # Select the Warehouse
    cs.execute(f"USE WAREHOUSE {snowflake_context.warehouse}")
    # Select the Database
    cs.execute(f"USE DATABASE {snowflake_context.database}")

    return cs

def get_config_file():
    """
    Input : No input
    
    Objective : Gets all the configurations in the config file.
    
    Output : Configurations    
    """
    project_name = "HP"
    # team = "team2"
    # BUName = "core"
    user_name='decision_node'
    job_name='inf'
    env = 'dev'
    
    config_bucket = f"dlk-cloud-tier-8-code-ml-{env}"  # Change this to point to the s3 location of your raw input data.
    
    config_s3_prefix = f'config_files/pipeline_config/{project_name}/{user_name}/{job_name}/dn_config.json'  # Change this to point to the s3 location of your raw input data.
    
    s3_client = boto3.client('s3')
    csv_obj = s3_client.get_object(Bucket=config_bucket, Key=config_s3_prefix)
    
    body = csv_obj['Body']
    
    json_string = body.read().decode('utf-8')
    json_content = json.loads(json_string)
    
    return json_content