from connectors.postgres import PostgresConnector
from extractor.kafka_producer import KafkaProducerClient
from connectors.config import Config


def run_connector():
    """
    Main entrypoint for the Postgres connector service.
    - Lists all tables in the Postgres database
    - Sends Kafka messages to schedule extraction for each table
    """

    print("🚀 Starting Postgres Connector Service")
    connector = PostgresConnector()
    producer = KafkaProducerClient()

    # List all entities (tables)
    tables = connector.list_entities()
    print(f"🔍 Found tables: {tables}")

    # Loop through tables and send extraction message
    for table in tables:
        df = connector.extract_data(entity=table, key_column="id", partitions=8)

        raw_path = connector.write_raw(df, table)

        # send metadata to Kafka
        producer.send_message({
            "source": "postgres",
            "table_name": table,
            "raw_path": raw_path,
            "status": "raw_written"
        })

        print(f"📤 Kafka message sent → loader will write Iceberg for {table}")

    connector.close()
#     for table in tables:
#         message = {
#             "source": "postgres",
#             "table_name": table,
#             "schema": "public",
#             "status": "pending",
#             "config": {
#                 "host": Config.POSTGRES_HOST,
#                 "port": Config.POSTGRES_PORT,
#                 "db": Config.POSTGRES_DB,
#                 "user": Config.POSTGRES_USER,
#                 "password": Config.POSTGRES_PASSWORD,
#             },
#         }

#         print(f"📤 Sending extraction trigger for table: {table}")
#         producer.send_message(message)

#     print("✅ All tables scheduled for extraction.")


if __name__ == "__main__":
    run_connector()

