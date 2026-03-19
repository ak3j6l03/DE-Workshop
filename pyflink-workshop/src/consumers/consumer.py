import sys
import json
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import ride_deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='green-trips-console',
    consumer_timeout_ms=5000
)

count = 0

for message in consumer:
    ride = message.value
    
    if ride['trip_distance'] > 5.0:
        count += 1
        print(ride)

print(f"Trips with distance > 5 km: {count}")

consumer.close()