import json
import logging
import os
import sys
from kafka import KafkaConsumer

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("consumer")

BOOTSTRAP_SERVERS = os.getenv(
    "BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092"
).split(",")
TOPICS = os.getenv(
    "KAFKA_TOPICS", "dbserver1.public.users,dbserver1.public.orders"
).split(",")
GROUP_ID = os.getenv("CONSUMER_GROUP", "demo-consumer-group")


def main():
    logger.info("Starting consumer. Brokers: %s", BOOTSTRAP_SERVERS)
    logger.info("Subscribing to topics: %s", TOPICS)
    logger.info("Consumer group: %s", GROUP_ID)

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        key_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        consumer_timeout_ms=5000,
    )

    logger.info("Consumer started, waiting for messages...")

    try:
        for msg in consumer:
            event = {
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "key": msg.key,
                "value": msg.value,
            }
            logger.info(json.dumps(event, ensure_ascii=False, default=str))
            print(json.dumps(event, ensure_ascii=False, default=str), flush=True)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
    except Exception:
        logger.exception("Error during consumption")
        sys.exit(1)
    finally:
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    main()