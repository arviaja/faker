import time
import random
import yaml
from datetime import datetime
from elasticsearch import Elasticsearch
from faker import Faker

# Load configuration from config.yaml
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Initialize Elasticsearch client with authentication
es = Elasticsearch(
    [{'host': config['elasticsearch']['host'], 'port': config['elasticsearch']['port']}],
    http_auth=(config['elasticsearch']['user'], config['elasticsearch']['password'])
)

# Initialize Faker for generating random data
fake = Faker()

# Function to generate a random log
def generate_log():
    log = {
        "@timestamp": datetime.now().isoformat(),
        "log_level": random.choice(["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"]),
        "message": fake.sentence(),
        "source": fake.hostname(),
        "user": fake.user_name(),
        "ip": fake.ipv4(),
        "response_time": round(random.uniform(0.1, 1.5), 3)
    }
    return log

# Function to send log to Elasticsearch
def send_log(log):
    # Construct index name with current date
    index_name = f"{config['index']}-{datetime.now().strftime('%Y.%m.%d')}"
    es.index(index=index_name, body=log)

# Number of logs to generate
num_logs = config['log_generation']['num_logs']

# Continuous or limited log generation
if num_logs == -1:
    while True:
        log = generate_log()
        send_log(log)
        print(f"Sent log: {log}")
        time.sleep(random.uniform(config['log_generation']['interval_min'], config['log_generation']['interval_max']))
else:
    for _ in range(num_logs):
        log = generate_log()
        send_log(log)
        print(f"Sent log: {log}")
        time.sleep(random.uniform(config['log_generation']['interval_min'], config['log_generation']['interval_max']))