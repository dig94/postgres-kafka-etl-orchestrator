import os

class Config:
    # PostgreSQL
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "35.207.193.219")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB = os.getenv("POSTGRES_DB", "test_db")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "remote_user")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "strong_password")


    MYSQL_HOST = os.getenv("MYSQL_HOST", "35.207.193.219")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_DB = os.getenv("MYSQL_DB", "test_db")
    MYSQL_USER = os.getenv("MYSQL_USER", "remote_user")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "strong_password")

    # Kafka
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "extract_queue")

    # Spark / JDBC
    #JDBC_JAR = os.getenv("JDBC_JAR", "/opt/jars/postgresql-42.2.27.jar")
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

    # MinIO or S3 storage config (optional)
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ACCESS = os.getenv("MINIO_ACCESS", "minio")
    MINIO_SECRET = os.getenv("MINIO_SECRET", "minio123")

    # Other
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
