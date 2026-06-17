import logging
from confluent_kafka import Producer

try:
    from kafka_pipeline.config.settings import settings
except ImportError:
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__),'..','..'))
    from kafka_pipeline.config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseProducer:
    """
    A resuable Kafka Producer class that handles configuration,
    message delivery reports, and graceful shutdowns.
    """
    def __init__(self):
        # 1. Producer Configuration
        self.conf = {
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'crypto-tracker-producer',
            'acks': 'all', # 'all' ensures high data reliability (all brokers must acknowledge)
            'retries': 5, 
            'retry.backoff.ms': 100 # 'retries' and 'retry.backoff.ms' help handle transient network issues
        }

        try:
            self.producer = Producer(self.conf)
            logger.info(f"Connected to Kafka at {settings.KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.error(f"Failed to create Kafka Producer: {e}")
            raise
        
    def delivery_report(self, err, msg):
        """
            Callback triggered by poll() once a message is successfully delivered
            or fails after retries.
        """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    def produce(self, topic: str, key: str, value: str):
        """
            Asynchronously sends a message to a Kafka topic.
        """
        try:
            # produce() is non-blocking; it puts the message in a local queue
            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=self.delivery_report
            )
            # poll(0) serves delivery report callbacks from previous produce calls
            self.producer.poll(0)
        except BufferError:
            # If the internal queue is full, we wait for 1 second and try to poll again
            logger.warning("Local producer queue is full, waiting...")
            self.producer.poll(1)
        except Exception as e:
            logger.error(f"Unexpected error during production: {e}")
        
    def flush(self, timeout=10):
        """
            Wait for all messages in the local queue to be delivered.
            Crucial to call this before the program exits.
        """
        logger.info("Flushing remaining mesages...")
        self.producer.flush(timeout)