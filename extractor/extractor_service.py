from connectors.registry import ConnectorRegistry
from extractor.kafka_producer import KafkaProducerClient


class ExtractorService:

    def __init__(self, source_type, config):
        self.source_type = source_type
        self.config = config
        self.producer = KafkaProducerClient()

        # Load connector dynamically (postgres, mysql, mongo, s3…)
        self.connector = ConnectorRegistry.get_connector(source_type, **config)

    def run_for_entity(self, entity, key_column="id", partitions=8):
        print(f"[EXTRACT] Starting extraction for: {entity}")

        # Read via JDBC (or API for NoSQL / S3)
        df = self.connector.extract_data(
            entity=entity,
            key_column=key_column,
            partitions=partitions
        )

        # Write raw parquet to MinIO
        raw_path = self.connector.write_raw(df, entity, source=self.source_type)

        # Produce event for loader service
        message = {
            "source": self.source_type,
            "table": entity,
            "raw_path": raw_path,
            "status": "raw_ready"
        }

        self.producer.send_message(message)

        print(f"[EXTRACT] Completed {entity} → {raw_path}")
        return raw_path

    def run_all(self, key_column="id"):
        entities = self.connector.list_entities()
        print(f"[EXTRACT] Discovered tables: {entities}")

        for entity in entities:
            self.run_for_entity(entity, key_column=key_column)

        print("[EXTRACT] All tables processed.")


# def extract_generic(source_type: str, config: dict, entity: str, key_column="id", partitions=8):
#     """
#     Generic extraction function that:
#     1. Uses ConnectorRegistry to create the correct connector (Postgres, MySQL, etc.)
#     2. Extracts data via Spark JDBC
#     3. Writes results as Parquet to S3/MinIO
#     4. Publishes metadata message to Kafka
#     """
#     print(f"[INFO] Starting extraction for source={source_type}, entity={entity}")

#     connector = ConnectorRegistry.get_connector(source_type, **config)
#     df = connector.extract_data(entity=entity, key_column=key_column, partitions=partitions)
#     record_count = df.count()
#     print(f"[INFO] Extracted {record_count} records from {entity} ({source_type})")

#     output_path = f"s3a://raw/{source_type}/{entity}/"
#     (
#         df.write
#         .mode("overwrite")
#         .parquet(output_path)
#     )
#     print(f"[INFO] Wrote extracted data to {output_path}")

#     # 4️⃣ Publish Kafka message
#     producer = KafkaProducerClient()
#     message = {
#         "source": source_type,
#         "entity": entity,
#         "output_path": output_path,
#         "records": record_count,
#         "status": "completed"
#     }
#     producer.send_message(message)

#     print(f"[INFO] ✅ Completed extraction for {entity} ({source_type})")
#     return output_path

