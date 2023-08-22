import os

# PATH
BASE_DIR = os.path.join(os.path.dirname( __file__ ), '..')

# DATETIME
DT_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

# MQTT
MQTT_HOST = '192.168.0.62'
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60
PC_PUBLISH_TOPIC = 'bedroom/torrent7'
TOPIC = 'bedroom/#'

# SQLITE
DATABASE_FILE = 'multisensors.db'
SQLITE_TABLE_NAME = 'multi_sensors_data'

# SENSORS
SENSOR_NAMES_SET = {
    'fan_cpu__measure',
    'fan_gpu__fan1',
    'fan_gpu__fan2',
    'fan_gpu__fan3',
    'sys_cpu__clock_core_max',
    'sys_cpu__clock_core_min',
    'sys_cpu__clock',
    'sys_cpu__utilization_thread_max',
    'sys_cpu__utilization_thread_min',
    'sys_cpu__utilization',
    'sys_gpu__mem_clock',
    'sys_gpu__mem_usage',
    'sys_gpu__utilization',
    'sys_mem__clock',
    'sys_mem__usage',
    'temp_chipset__measure',
    'temp_cpu__measure',
    'temp_gpu__hotspot',
    'temp_gpu__measure',
    'temp_hdd__hdd1',
    'temp_hdd__hdd2',
    'temp_mobo__measure',
    'voltage_cpu__measure',
    'voltage_gpu__measure',
    'wattage_cpu__measure',
    'wattage_gpu__measure'
}
SORTED_SENSORS_LIST = sorted(list(SENSOR_NAMES_SET))

# AWS
AWS_PROFILE = 'test'
CONFIG_MAX_ATTEMPTS = 10
CONFIG_MAX_POOL_CONNECTION = 5000
CONFIG_READ_TIMEOUT = 20
DATABASE_NAME = 'testsensors'
MAX_RECORDS_PER_WRITE = 100
REGION_NAME = 'us-east-2'
TABLE_NAME = 'multimetrics'

# FLASK
FLASK_PORT = 3000
