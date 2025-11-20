from confluent_kafka import Producer
import json
import sys
from connectors.config import Config


class KafkaProducerClient:
    """
    Kafka Producer wrapper using confluent_kafka.
    Sends JSON-encoded messages to the configured topic.
    """

    def __init__(self):
        try:
            self.topic = Config.KAFKA_TOPIC
            self.producer = Producer({"bootstrap.servers": Config.KAFKA_BROKER})
            print(f"[INFO] KafkaProducer connected to {Config.KAFKA_BROKER}, topic={self.topic}")
        except Exception as e:
            print(f"[ERROR] Failed to initialize Kafka producer: {e}")
            sys.exit(1)

    def delivery_report(self, err, msg):
        """Callback triggered for each message delivery."""
        if err:
            print(f"[ERROR] Delivery failed: {err}")
        else:
            print(
                f"[INFO] Delivered to topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
            )

    def send_message(self, message: dict):
        """Send a JSON-encoded message to Kafka."""
        try:
            value = json.dumps(message).encode("utf-8")
            self.producer.produce(
                topic=self.topic,
                value=value,
                callback=self.delivery_report
            )
            #self.producer.poll(0)  # trigger delivery callbacks
        # except BufferError as e:
        #     print(f"[WARN] Buffer full, flushing... {e}")
        #     self.producer.flush()
        #     self.producer.produce(
        #         topic=self.topic,
        #         value=value,
        #         callback=self.delivery_report
        #     )
        except Exception as e:
            print(f"[ERROR] Failed to send message: {e}")
        finally:
            self.producer.flush()

        print(f"[INFO] ✅ Sent message to {self.topic}: {message}")
