from confluent_kafka import Producer
import json, time, os
from confluent_kafka.admin import AdminClient, NewTopic

# Admin client config
admin_conf = {'bootstrap.servers': 'localhost:9092'}
admin_client = AdminClient(admin_conf)

# Create topic
new_topic = NewTopic("kindle-reviews", num_partitions=1, replication_factor=1)
fs = admin_client.create_topics([new_topic])

for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic {topic} created successfully")
    except Exception as e:
        print(f"Failed to create topic {topic}: {e}")

# Producer config
producer_conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "yelp-json-producer"
}

# Initialize Kafka producer once
producer = Producer(producer_conf)


# Kafka topic
topic = "kindle-reviews"

# Optional: delivery report callback
def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} | key: {msg.key().decode()}")

# Read checkpoint to resume from last sent line
def read_checkpoint(path):
    return int(open(path).read()) if os.path.exists(path) else 0

# Write checkpoint after each sent message
def write_checkpoint(path, index):
    with open(path, "w") as f:
        f.write(str(index))

# Stream the JSON file line by line to Kafka
def stream_reviews(file_path, checkpoint_path="checkpoint.txt", sleep_time=1):
    last_index = read_checkpoint(checkpoint_path)

    with open(file_path, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            if idx < last_index:
                continue  # Skip already sent
            try:
                review = json.loads(line)

                # Produce to Kafka
                producer.produce(
                    topic=topic,
                    key=str(review.get("reviewerID", f"line_{idx}")),
                    value=json.dumps(review).encode("utf-8"),
                    callback=delivery_report
                )
                producer.flush()

                write_checkpoint(checkpoint_path, idx + 1)

                time.sleep(sleep_time)  # simulate real-time stream

            except json.JSONDecodeError:
                print(f"Invalid JSON at line {idx}, skipping.")
            except Exception as e:
                print(f"Error at line {idx}: {e}")

# Entry point
if __name__ == "__main__":
    stream_reviews("data/kindle_reviews.json")
