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
        #self.jdbc_jar = Config.JDBC_JAR

        self.jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"

        # Initialize Spark
        # self.spark = (
        #     SparkSession.builder.appName("PostgresJDBCExtractor").config("spark.master","local[*]")
        #     .config("spark.sql.shuffle.partitions", "8")
        #     .getOrCreate()
        # )

        self.spark= SparkSession.builder \
            .appName("PostgresJDBCExtractor") \
            .config("spark.master","local[*]") \
            .config("spark.sql.shuffle.partitions","8") \
            .config("spark.hadoop.fs.s3a.endpoint","http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key","minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key","minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access","true") \
            .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false") \
            .getOrCreate()

        self.props = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }

    def list_entities(self):
        """List all public tables in the PostgreSQL database."""
        query = "(SELECT table_name FROM information_schema.tables WHERE table_schema='public') AS t"
        df = self.spark.read.jdbc(self.jdbc_url, query, properties=self.props)
        return [r["table_name"] for r in df.collect()]

    # def get_bounds(self, table, id_column="id"):
    #     """Find MIN and MAX of a numeric primary key for partitioning."""
    #     query = f"(SELECT MIN({id_column}) AS min_id, MAX({id_column}) AS max_id FROM {table}) AS bounds"
    #     df = self.spark.read.jdbc(self.jdbc_url, query, properties=self.props)
    #     row = df.collect()[0]
    #     return int(row["min_id"] or 0), int(row["max_id"] or 0)

    # def read_range(self, table, key_column, start, end):
    #     """Read one range (partition) of data."""
    #     query = f"(SELECT * FROM {table} WHERE {key_column} >= {start} AND {key_column} < {end}) AS chunk"
    #     return self.spark.read.jdbc(self.jdbc_url, query, properties=self.props)

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
            # Fallback for tables missing the 'key_column' (e.g., 'id')
            if f"Column public.{entity}.{key_column} not found" in str(e):
                print(f"[WARN] Key column '{key_column}' not found or invalid in table {entity}. Falling back to single partition read.")
                # Reading without specifying partitioning parameters results in a single partition fetch.
                df = self.spark.read.jdbc(self.jdbc_url, entity, properties=self.props)
                
                # --- Robustness Enhancement: Add a synthetic key if the original key is missing ---
                
                df = df.withColumn("_synth_id", monotonically_increasing_id())
                print("[INFO] Added synthetic column '_synth_id' for stability.")
                return df

            # Re-raise other errors, including the Py4JJavaError from `hash(text)` if it persists
            raise
        # print(f"[INFO] Extracting {entity} using adaptive JDBC partitioning on {key_column}")
        # return adaptive_read(
        #     spark=self.spark,
        #     jdbc_url=self.jdbc_url,
        #     db_flavor="postgres",
        #     table=entity,
        #     key_column=key_column,
        #     props=self.props,
        #     partitions=partitions,
        #     schema="public",
        # )

    # def extract_data(self, entity, key_column="id", partitions=8):
    #     """Main extraction entrypoint using Spark JDBC."""
    #     lo, hi = self.get_bounds(entity, key_column)

    #     if lo == hi:
    #         print(f"[WARN] Table {entity} appears empty or constant key, using single partition.")
    #         return self.spark.read.jdbc(self.jdbc_url, entity, properties=self.props)

    #     total = hi - lo
    #     num_parts = 1 if total < 10000 else min(partitions, total // 1_000_000 + 1)
    #     print(f"[INFO] Extracting {entity} ({lo}-{hi}) using {num_parts} partitions via Spark JDBC.")

    #     df = self.spark.read.jdbc(
    #         url=self.jdbc_url,
    #         table=entity,
    #         column=key_column,
    #         lowerBound=lo,
    #         upperBound=hi,
    #         numPartitions=num_parts,
    #         properties=self.props,
    #     )
    #     return df

    # ----------------------------------------------------------------------
    # 4. WRITE RAW DATA TO MINIO
    # ----------------------------------------------------------------------
    def write_raw(self, df, entity, source="postgres"):
        """Write extracted data to MinIO as Parquet in raw zone."""
        output_path = f"s3a://raw/{source}/{entity}/"
        print(f"[INFO] Writing Parquet to MinIO: {output_path}")
        df.write.mode("overwrite").parquet(output_path)
        return output_path

    def close(self):
        """Stop Spark session."""
        self.spark.stop()
