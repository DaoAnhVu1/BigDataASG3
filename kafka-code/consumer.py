from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Replace with actual IP of where Kafka is hosted
KAFKA_BROKER = "54.167.39.188:9092"
KAFKA_TOPIC = "movies_ratings"

MONGO_URI = "mongodb+srv://s3926187:bigdataasg3@bigdataasg3.ih4zw.mongodb.net/?retryWrites=true&w=majority&appName=BigDataASG3"
DB_NAME = "movies"
COLLECTION_NAME = "rating"

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Read messages from the beginning
    enable_auto_commit=True
)

print(f"Connected to Kafka topic: {KAFKA_TOPIC}")
print(f"Ingesting into MongoDB database: {DB_NAME}, collection: {COLLECTION_NAME}")

try:
    for message in consumer:
        record = message.value
        print(f"Consumed message: {record}")
        collection.insert_one(record)
        print("Inserted into MongoDB")
except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    consumer.close()
    mongo_client.close()