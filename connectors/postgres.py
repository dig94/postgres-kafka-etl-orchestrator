from connectors.base_connector import BaseConnector
from connectors.config import Config
from helper.jdbc_partioning import adaptive_read
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id


class PostgresConnector(BaseConnector):
    """
    Spark JDBC-based PostgreSQL Connector.
    Works efficiently for both small and large tables.
    """

    def __init__(self):
        self.host = Config.POSTGRES_HOST
        self.port = Config.POSTGRES_PORT
        self.db = Config.POSTGRES_DB
        self.user = Config.POSTGRES_USER
        self.password = Config.POSTGRES_PASSWORD

        self.jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"

        # -------------------------------
        # Build Spark session with S3A / MinIO support
        # -------------------------------
        minio_host = Config.MINIO_ENDPOINT.replace("http://", "").replace("https://", "")

        self.spark = (
            SparkSession.builder
            .appName("PostgresJDBCExtractor")
            .config("spark.master", "local[*]")
            .config("spark.sql.shuffle.partitions", "8")

            # ---- S3A + Hadoop configs ----
            .config("spark.hadoop.fs.s3a.endpoint", minio_host)
            .config("spark.hadoop.fs.s3a.access.key", Config.MINIO_ACCESS)
            .config("spark.hadoop.fs.s3a.secret.key", Config.MINIO_SECRET)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

            # Required for avoiding AWS V4 signing errors
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

            .getOrCreate()
        )

        # JDBC properties
        self.props = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }

    # -------------------------------
    # LIST TABLES
    # -------------------------------
    def list_entities(self):
        query = "(SELECT table_name FROM information_schema.tables WHERE table_schema='public') AS t"
        df = self.spark.read.jdbc(self.jdbc_url, query, properties=self.props)
        return [r["table_name"] for r in df.collect()]

    # -------------------------------
    # EXTRACT TABLE DATA WITH ADAPTIVE PARTITIONING
    # -------------------------------
    def extract_data(self, entity, key_column="id", partitions=8):
        try:
            print(f"[INFO] Extracting {entity} using adaptive JDBC partitioning on {key_column}")
            return adaptive_read(
                spark=self.spark,
                jdbc_url=self.jdbc_url,
                db_flavor="postgres",
                table=entity,
                key_column=key_column,
                props=self.props,
                partitions=partitions,
                schema="public",
            )

        except ValueError as e:
            # Column missing → fallback mode
            if f"Column public.{entity}.{key_column} not found" in str(e):
                print(f"[WARN] Key column '{key_column}' not found in {entity}. Using single-partition read.")
                df = self.spark.read.jdbc(self.jdbc_url, entity, properties=self.props)

                df = df.withColumn("_synth_id", monotonically_increasing_id())
                print("[INFO] Added synthetic column '_synth_id' for stability.")
                return df

            raise  # bubble up real errors

    # -------------------------------
    # WRITE RAW DATA TO MINIO
    # -------------------------------
    def write_raw(self, df, entity, source="postgres"):
        output_path = f"s3a://raw/{source}/{entity}/"
        print(f"[INFO] Writing Parquet to MinIO: {output_path}")

        df.write.mode("overwrite").parquet(output_path)

        return output_path

    def close(self):
        self.spark.stop()
