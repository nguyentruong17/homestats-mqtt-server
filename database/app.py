#!/usr/bin/env python3

from aws_utils import (
    get_aws_write_client,
    prepare_common_attributes,
    prepare_measure,
    print_rejected_records_exceptions
)
from constants import (
    DATABASE_FILE,
    DATABASE_NAME,
    FLASK_PORT,
    MAX_RECORDS_PER_WRITE,
    MQTT_HOST,
    MQTT_KEEPALIVE,
    MQTT_PORT,
    PC_PUBLISH_TOPIC,
    SENSOR_NAMES_SET,
    SORTED_SENSORS_LIST,
    SQLITE_TABLE_NAME,
    TABLE_NAME,
    TOPIC
)
from datetime import datetime
from flask import Flask, request
from thread_utils import run_threaded
import json
import paho.mqtt.client as mqtt
import schedule
import sqlite3
import threading
import time

FLASK_APP = Flask(__name__)

def on_connect(client, userdata, flags, rc):
    print(f'Connected with result code {rc}')

    client.subscribe(TOPIC)

def on_disconnect(client):
    print('Disconnected')

def on_message(client, userdata, msg):
    topic = msg.topic

    if topic == PC_PUBLISH_TOPIC:
        message=msg.payload.decode('utf-8')
        
        payloadJson = json.loads(message)
        timestamp = payloadJson['sent']
        delimitter = payloadJson['delimitter']
        sensors = payloadJson['payload']
        
        print(f'Message with topic "{topic}" received at: {timestamp}')
        # json_formatted_str = json.dumps(payloadJson, indent=4)
        # print(f'message received at: {timestamp}. {json_formatted_str}')

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
    else:
        utc = datetime.utcnow()
        print(f'Message with topic "{topic}" received at {utc} does not have any handler')

def get_db_conn(use_row = False):
    db_conn = sqlite3.connect(DATABASE_FILE, check_same_thread=False)
    
    if use_row:
        db_conn.row_factory = sqlite3.Row 
    
    return db_conn

def init_sql_table():
    print(f'init_sql_table started')
    cols = map(lambda col: col + ' REAL NOT NULL', SORTED_SENSORS_LIST)
    sql = ', '.join(cols)

    # Initialize SQLITE3
    db_conn = get_db_conn()
    sql = f"""
        CREATE TABLE IF NOT EXISTS {SQLITE_TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            {sql}
        )
    """
    
    cursor = db_conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS {SQLITE_TABLE_NAME}')
    cursor.execute(sql)
    db_conn.commit()
    cursor.close()
    
    print(f'init_sql_table completed')
    
    return db_conn

def connect_mqtt(userdata):
    print('connect_mqtt started')
    client = mqtt.Client()
    
    # db_conn = userdata['db_conn']
    
    client.user_data_set(userdata)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    
    connected = False
    while not connected:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)
            client.loop_start()
            
            connected = True
        except Exception as err:
            print(f'connect_mqtt error: {err}')
            time.sleep(4)
            print('connect_mqtt retrying...')
    
    print('connect_mqtt completed')

def init_flask(userdata):
    print(f'init flask at port {FLASK_PORT}')
    
    FLASK_APP.run(host='0.0.0.0', port=FLASK_PORT, debug=True, use_reloader=False)

@FLASK_APP.route('/metrics', methods = ['GET'])
def index():
    filter_minutes = request.args.get('minutes', default = 1, type = int)
    filter_group = request.args.get('group', default = '*', type = str)
    filter_group = filter_group.lower()
    filter_sensor = request.args.get('sensor', default = '*', type = str)
    filter_sensor = filter_sensor.lower()

    db_conn = get_db_conn(True)
    sql = f"SELECT * FROM {SQLITE_TABLE_NAME} WHERE timestamp >= datetime('now', '-{filter_minutes} minute')"
    cursor = db_conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    
    json_string = json.dumps([dict(row) for row in rows]) 
    return json_string

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

    if (len(records) == 0):
        print('write_records: no record to write')
        return

    print(f'write_records length: {len(records)}')    
    print(f'write_records first: {records[0]}')
    print(f'write_records last: {records[-1]}')
    
    # https://www.geeksforgeeks.org/break-list-chunks-size-n-python/
    batches = [records[i * MAX_RECORDS_PER_WRITE:(i + 1) * MAX_RECORDS_PER_WRITE] for i in range((len(records) + MAX_RECORDS_PER_WRITE - 1) // MAX_RECORDS_PER_WRITE )]
    
    print(f'write_records batches: {len(batches)}.')
    print('write_records started')
    common_attributes = prepare_common_attributes()
    for batch in batches: 
        try:
            result = write_client.write_records(
                DatabaseName=DATABASE_NAME,
                TableName=TABLE_NAME,
                Records=batch,
                CommonAttributes=common_attributes
            )
            print(f"write_recordsstatus: [{result['ResponseMetadata']['HTTPStatusCode']}]")
        except write_client.exceptions.RejectedRecordsException as err:
            print_rejected_records_exceptions(err)
        except Exception as err:
            print(f'write_records error: {err}')
    print('write_records completed')

def run_threaded(job_func, userdata):
    job_thread = threading.Thread(target=job_func, args=(userdata,))
    job_thread.start()
    
def publish_remote(userdata):
    print('publish_remote started')
    db_conn = userdata['db_conn']

    sql = f"SELECT * FROM {SQLITE_TABLE_NAME} WHERE timestamp >= datetime('now', '-1 hour')"
    cursor = db_conn.cursor()
    cursor.execute(sql)
    
    rows = cursor.fetchall()
    cursor.close()
    
    print(f'numrows: {len(rows)}')
    write_records(rows)
    print('publish_remote completed')

def clear_local(userdata):
    print('clear_local started')
    db_conn = userdata['db_conn']
    sql = f"DELETE FROM {SQLITE_TABLE_NAME} WHERE timestamp <= datetime('now','-2 days')"
    cursor = db_conn.cursor()
    cursor.execute(sql)
    print(f'clear_local total: {cursor.rowcount}')
    db_conn.commit()
    cursor.close()
    print('clear_local completed')

print(f'initilizing...')

db_conn = init_sql_table()
userdata={'db_conn': db_conn}

connect_mqtt(userdata)
run_threaded(init_flask, userdata=userdata)

print(f'\ninitilizing completed!')

schedule.every(1).hours.do(run_threaded, publish_remote, userdata=userdata)
schedule.every(1).days.do(run_threaded, clear_local, userdata=userdata)

while True:
    schedule.run_pending()
    time.sleep(1)
