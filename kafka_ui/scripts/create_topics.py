import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__),'..','src'))

from confluent_kafka.admin import AdminClient, NewTopic
from kafka_pipeline.config.settings import settings

def create_topic(topic_name):
    conf = {'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS}
    admin_client = AdminClient(conf)

    # Define  topic: name, number of partitions, replication factor
    new_topic = NewTopic(topic_name, num_partitions=3, replication_factor=3)

    # Call create_topics
    fs = admin_client.create_topics([new_topic])

    for topic, f in fs.items():
        try:
            f.result() # the result itself is none
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            print(f"Topic '{topic}' already exists or failed: {e}")

if __name__ == "__main__":
    create_topic("market_events")