import asyncio
import websockets
import json
import logging

try:
    from kafka_pipeline.producer.base_producer import BaseProducer
    from kafka_pipeline.config.settings import settings
except ImportError:
    import sys
    import os
    # Add 'kafka_ui/src' to sys.path
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
    from kafka_pipeline.producer.base_producer import BaseProducer
    from kafka_pipeline.config.settings import settings

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def stream_to_kafka():
    # 1. Initialize our Kafka Producer
    producer = BaseProducer()
    topic = settings.KAFKA_TOPIC_NAME

    uri = "wss://stream.binance.com:9443/ws/btcusdt@ticker"

    try:
        logger.info(f"Connecting to Binance WebSocket: {uri}")
        async with websockets.connect(uri) as ws:
            while True:
                # 2. Receive message from Binance
                message = await ws.recv()

                    # 3. Produce to Kafka
                # We use 'BTC-USDT' as the key so all trades for this pair
                # go to the same Kafka partition in order.
                producer.produce(
                    topic=topic,
                    key="BTC-USDT",
                    value=message
                )

                # Optional: keep the print so you can see it in your terminal
                print(f"Sent to Kafka: {message[:50]}...")
    except asyncio.CancelledError:
        logger.info("Streaming cancelled by user.")
    except Exception as e:
        logger.error(f"Streaming error: {e}")
    finally:
        # 4. CRITICAL: Flush messages before closing
        # This ensures Kafka receives the last few messages in the buffer
        producer.flush()


if __name__ == "__main__":
    try:
        asyncio.run(stream_to_kafka())
    except KeyboardInterrupt:
        pass
