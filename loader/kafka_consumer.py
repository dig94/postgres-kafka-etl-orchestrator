from confluent_kafka import Consumer, KafkaException
import json
from loader.loader_service import LoaderService
from connectors.config import Config


def get_kafka_consumer():
    return Consumer({
        "bootstrap.servers": Config.KAFKA_BROKER,
        "group.id": "loader-group",
        "auto.offset.reset": "earliest"
    })


def start_loader_consumer():
    print("🔄 Starting Loader Kafka Consumer...")

    consumer = get_kafka_consumer()
    loader = LoaderService()

    consumer.subscribe([Config.KAFKA_TOPIC])
    print(f"📡 Subscribed to Kafka topic: {Config.KAFKA_TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"⚠️ Kafka error: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                print(f"📩 Received message: {payload}")

                # Extract fields safely
                source = payload.get("source")
                table = payload.get("table_name") or payload.get("table")
                raw_path = payload.get("output_path") or payload.get("raw_path")

                if not all([source, table, raw_path]):
                    print(f"❌ Missing required keys in payload: {payload}")
                    continue

                # Run Iceberg loader
                loader.load_to_iceberg(
                    source=source,
                    table=table,
                    raw_path=raw_path
                )

                consumer.commit()

            except Exception as e:
                print(f"❌ Error processing message: {e}")

    except KeyboardInterrupt:
        print("🛑 Shutdown signal received.")

    finally:
        consumer.close()
        loader.stop()
        print("👋 Loader Consumer shutdown complete.")


# THIS WAS MISSING
if __name__ == "__main__":
    start_loader_consumer()