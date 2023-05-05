#!/usr/bin/env python3
import boto3
from botocore.config import Config
import configparser
import json
import os
import paho.mqtt.client as mqtt
import re
import sqlite3

# PATH
BASE_DIR = os.path.join(os.path.dirname( __file__ ), '..')

# MQTT
MQTT_HOST = '192.168.0.62'
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60
TOPIC = 'bedroom/#'

# SQLITE
DATABASE_FILE = 'sensors.db'

# AWS
AWS_PROFILE = 'test'
DATABASE_NAME = 'testsensors'
REGION_NAME = 'us-east-2'
TABLE_NAME = 'metrics'

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

def write_records(rows):
    write_client = get_aws_write_client()

    if (write_client is None): return
    
    records = []

    for row in rows:
        # print(row)
        sqlite_id, timestamp, rgroup, sensor, measure_name, measure_value = row
        
        dimensions = [
            {'Name': 'rgroup', 'Value': rgroup},
            {'Name': 'sensor', 'Value': sensor} 
        ]
        
        record = {
            'Dimensions': dimensions,
            'MeasureName': measure_name,
            'MeasureValue': str(measure_value),
            'MeasureValueType': 'DOUBLE',
            'Time': timestamp
        }
        
        print(record)
        
        records.append(record)
        
    if (len(records) == 0):
        print('No record to write.')
        return
    
    print('Writing records')
    try:
        result = write_client.write_records(
            DatabaseName=DATABASE_NAME,
            TableName=TABLE_NAME,
            Records=records,
            CommonAttributes={}
        )
        print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
    except write_client.exceptions.RejectedRecordsException as err:
        print_rejected_records_exceptions(err)
    except Exception as err:
        print("Error:", err)
    
def on_connect(client, userdata, flags, rc):
    print('Connected with result code ' + str(rc))

    client.subscribe(TOPIC)
    
    db_conn = userdata['db_conn']
    sql = "SELECT * FROM sensors_data WHERE timestamp >= datetime('now', '-1 day')"
    cursor = db_conn.cursor()
    cursor.execute(sql)
    
    rows = cursor.fetchall()
    
    write_records(rows)
    

def on_disconnect(client):
    print('Disconnected')

def on_message(client, userdata, msg):
    message=msg.payload.decode('utf-8')
    #print(message)
    
    payloadJson = json.loads(message)
    timestamp = payloadJson['sent']
    sensors = payloadJson['payload']
    
    db_conn = userdata['db_conn']
    
    for each in sensors:
        id = each['Id']
        measure_value = each['Value']
        
        parts = re.split('\[(.*?)\]', id)
        
        segments = filter(lambda part: bool(part), parts)
        
        group, sensor, measure_name = segments
        
        measure_name = measure_name[1:]
        
        sql = 'INSERT INTO sensors_data (timestamp, rgroup, sensor, measure_name, measure_value) VALUES (?, ?, ?, ?, ?)'
        cursor = db_conn.cursor()
        cursor.execute(sql, (timestamp, group, sensor, measure_name, measure_value))
    
    db_conn.commit()
    cursor.close()

def main():
    # Initialize SQLITE3
    db_conn = sqlite3.connect(DATABASE_FILE)
    sql = """
        CREATE TABLE IF NOT EXISTS sensors_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            rgroup TEXT NOT NULL,
            sensor TEXT NOT NULL,
            measure_name TEXT NOT NULL,
            measure_value REAL NOT NULL
        )
    """
    cursor = db_conn.cursor()
    cursor.execute(sql)
    cursor.close()

    # Initialize the client that should connect to the Mosquitto broker
    print('Before Init MQTT')
    client = mqtt.Client()
    
    client.user_data_set({'db_conn': db_conn})

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)

    # Blocking loop to the Mosquitto broker
    client.loop_forever()

main()
