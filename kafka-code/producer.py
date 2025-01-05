import random
import datetime
import time
from kafka import KafkaProducer
import json

KAFKA_BROKER = "54.167.39.188:9092"
KAFKA_TOPIC = "movies_ratings"

def generate_random_data():
    user_id = random.randint(1, 500)
    movie_id = random.randint(1, 1000)
    rating = random.choice([1, 2, 3, 4, 5])
    sentiment_map = {
        1: "VeryBad",
        2: "Bad",
        3: "Neutral",
        4: "Good",
        5: "Excellent"
    }
    sentiment = sentiment_map[rating]
    date = (datetime.datetime.now() + datetime.timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
    
    return {
        "user_id": user_id,
        "movie_id": movie_id,
        "rating": rating,
        "sentiment": sentiment,
        "date": date
    }

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def push_to_kafka():
    try:
        while True:  
            message = generate_random_data()
            producer.send(KAFKA_TOPIC, value=message)
            print(f"Sent message: {message}")
            time.sleep(10)
    finally:
        producer.close()

if __name__ == "__main__":
    push_to_kafka()