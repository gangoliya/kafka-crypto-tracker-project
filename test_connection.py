from confluent_kafka.admin import AdminClient

def test_connection():
    conf = {'bootstrap.servers': 'localhost:9092'}
    admin_client = AdminClient(conf)
    
    try:
        # Fetch metadata to verify connectivity
        metadata = admin_client.list_topics(timeout=10)
        print("Successfully connected to Kafka!")
        print(f"Brokers found: {len(metadata.brokers)}")
        for b in metadata.brokers.values():
            print(f" - Broker {b.id}: {b.host}:{b.port}")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")

if __name__ == "__main__":
    test_connection()
