#!/usr/bin/env python3

# import configparser
# import re
from botocore.config import Config
from datetime import datetime
import boto3
import json
import os
import paho.mqtt.client as mqtt
import schedule
import sqlite3
import threading
import time

# PATH
BASE_DIR = os.path.join(os.path.dirname( __file__ ), '..')

# DATETIME
DT_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

# MQTT
MQTT_HOST = '192.168.0.62'
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60
TOPIC = 'bedroom/#'

# SQLITE
DATABASE_FILE = 'multisensors.db'
SQLITE_TABLE_NAME = 'multi_sensors_data'

# AWS
AWS_PROFILE = 'test'
DATABASE_NAME = 'testsensors'
MAX_RECORDS_PER_WRITE = 100
REGION_NAME = 'us-east-2'
TABLE_NAME = 'multimetrics'
            
# SENSORS
SENSOR_NAMES_SET = {
    'sys_cpu__utilization',
    'sys_mem__usage',
    'sys_gpu__mem_clock',
    'sys_gpu__utilization',
    'sys_gpu__mem_usage',
    'temp_mobo__measure',
    'temp_chipset__measure',
    'temp_gpu__measure',
    'temp_gpu__hotspot',
    'fan_cpu__measure',
    'fan_gpu__fan1',
    'fan_gpu__fan2',
    'fan_gpu__fan3',
    'voltage_gpu__measure',
    'wattage_gpu__measure',
    'sys_cpu__clock_core_max',
    'sys_cpu__clock_core_min',
    'sys_cpu__utilization_thread_max',
    'sys_cpu__utilization_thread_min',
    'temp_hdd__hdd1',
    'temp_hdd__hdd2'
}
SORTED_SENSORS_LIST = sorted(list(SENSOR_NAMES_SET))

def print_rejected_records_exceptions(err):
    print("RejectedRecords: ", err)
    for rr in err.response["RejectedRecords"]:
        print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
    if "ExistingVersion" in rr:
        print("Rejected record existing version: ", rr["ExistingVersion"])

def get_aws_write_client():
    profile_name=os.environ['AWS_PROFILE']
    
    session = boto3.Session(profile_name=profile_name)

    write_client = session.client(
            'timestream-write',
            region_name=REGION_NAME,
            config=Config(read_timeout=20,
                max_pool_connections=5000,
                retries={'max_attempts': 10}
            )
        )
    
    return write_client

def prepare_common_attributes():
    common_attributes = {
        'Dimensions': [
            {'Name': 'hostname', 'Value': 'Torrent7'}
        ],
        'MeasureName': 'metric',
        'MeasureValueType': 'MULTI'
    }
    
    return common_attributes

def prepare_measure(measure_name, measure_value):
    measure = {
        'Name': measure_name,
        'Value': str(measure_value),
        'Type': 'DOUBLE'
    }
    
    return measure

def write_records(rows):
    write_client = get_aws_write_client()

    if (write_client is None): return
    
    records = []

    for row in rows:
        sqlite_id, timestamp, *rest = row
        
        dt_obj = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
        time = int(round(dt_obj.timestamp() * 1000))
        
        record = {
            'Time': str(time),
            'MeasureValues': []
        }
        
        for index in range(len(SORTED_SENSORS_LIST)):
            metric = SORTED_SENSORS_LIST[index]
            value = rest[index]
            measure = prepare_measure(metric, value)
            
            record['MeasureValues'].append(measure)
              
        records.append(record)
     
    print(f'numrecords: {len(records)}')
    if (len(records) == 0):
        print('No record to write.')
        return
    
    print(f'lastrecod: {records[-1]}')
    
    # https://www.geeksforgeeks.org/break-list-chunks-size-n-python/
    batches = [records[i * MAX_RECORDS_PER_WRITE:(i + 1) * MAX_RECORDS_PER_WRITE] for i in range((len(records) + MAX_RECORDS_PER_WRITE - 1) // MAX_RECORDS_PER_WRITE )]
    print(f'numbatches: {len(batches)}')
    common_attributes = prepare_common_attributes()

    print('WriteRecords in progress...')    
    for batch in batches: 
        try:
            result = write_client.write_records(
                DatabaseName=DATABASE_NAME,
                TableName=TABLE_NAME,
                Records=batch,
                CommonAttributes=common_attributes
            )
            print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except write_client.exceptions.RejectedRecordsException as err:
            print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)

def on_connect(client, userdata, flags, rc):
    print('Connected with result code ' + str(rc))

    client.subscribe(TOPIC)

def on_disconnect(client):
    print('Disconnected')

def on_message(client, userdata, msg):
    message=msg.payload.decode('utf-8')
    
    payloadJson = json.loads(message)
    timestamp = payloadJson['sent']
    delimitter = payloadJson['delimitter']
    sensors = payloadJson['payload']
    
    print(f'message received at: {timestamp}')
    
    db_conn = userdata['db_conn']
    
    sensorsDict = dict.fromkeys(SENSOR_NAMES_SET, -1)
    
    for each in sensors:
        key = each['Id']
        value = each['Value']
        
        if key in sensorsDict:
            sensorsDict[key] = value
    
    cols = ', '.join(SORTED_SENSORS_LIST)
    
    vals = map(lambda col: ':' + col, SORTED_SENSORS_LIST)
    vals = ', '.join(vals)
    
    sql = f"""INSERT INTO {SQLITE_TABLE_NAME}
        (
            timestamp,
            {cols}
        ) VALUES
        (
            :timestamp,
            {vals}
        )
    """

    sensorsDict['timestamp'] = timestamp
    cursor = db_conn.cursor()
    cursor.execute(sql, sensorsDict)
    db_conn.commit()
    cursor.close()

def init_sql_table():
    cols = map(lambda col: col + ' REAL NOT NULL', SORTED_SENSORS_LIST)
    sql = ', '.join(cols)
    
    print(f'init sql table with cols {sql}.')
    # Initialize SQLITE3
    db_conn = sqlite3.connect(DATABASE_FILE, check_same_thread=False)
    sql = f"""
        CREATE TABLE IF NOT EXISTS {SQLITE_TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            {sql}
        )
    """
    
    cursor = db_conn.cursor()
    # cursor.execute(f'DROP TABLE IF EXISTS {SQLITE_TABLE_NAME}')
    cursor.execute(sql)
    cursor.close()
    
    print(f'init sql table completed.')
    
    return db_conn

def connect_mqtt(userdata):
    print('init mqtt connection thread.')
    client = mqtt.Client()
    
    db_conn = userdata['db_conn']
    
    client.user_data_set({'db_conn': db_conn})

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)

    client.loop_start()
    
    print('init mqtt connection thread completed.')

def run_threaded(job_func, userdata):
    job_thread = threading.Thread(target=job_func, args=(userdata,))
    job_thread.start()

def publish_local(userdata):
    db_conn = userdata['db_conn']

    sql = f"SELECT * FROM {SQLITE_TABLE_NAME} WHERE timestamp >= datetime('now', '-1 hour')"
    cursor = db_conn.cursor()
    cursor.execute(sql)
    
    rows = cursor.fetchall()
    db_conn.commit()
    cursor.close()
    
    print(f'numrows: {len(rows)}')
    print('publishing to remote')
    
    print('WriteRecords started')
    write_records(rows)
    print('WriteRecords completed')

def clear_local(userdata):
    print('clearing from local')

db_conn = init_sql_table()
userdata={'db_conn': db_conn}
connect_mqtt(userdata)

schedule.every(1).hours.do(run_threaded, publish_local, userdata=userdata)
# schedule.every(2).minutes.do(run_threaded, clear_local, userdata=userdata)

while True:
    schedule.run_pending()
    time.sleep(1)
