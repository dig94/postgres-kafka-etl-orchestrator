from pyspark.sql import SparkSession


def get_spark_for_loader():
    spark = (
        SparkSession.builder
        .appName("IcebergLoader")

        # Iceberg JDBC Catalog
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "jdbc")
        .config("spark.sql.catalog.iceberg.catalog-name", "iceberg")
        .config("spark.sql.catalog.iceberg.uri", "jdbc:postgresql://iceberg-postgres:5432/iceberg")
        .config("spark.sql.catalog.iceberg.jdbc.user", "remote_user")
        .config("spark.sql.catalog.iceberg.jdbc.password", "strong_password")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/")

        # Iceberg SQL extension
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

        # MinIO (S3A filesystem)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "miniouser")
        .config("spark.hadoop.fs.s3a.secret.key", "miniopassword")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # Avoid partitions explosion
        .config("spark.sql.shuffle.partitions", "4")

        .getOrCreate()
    )
    return spark


class LoaderService:
    """
    Converts raw MinIO Parquet → Iceberg tables.
    Uses JDBC catalog to store metadata in Postgres.
    """

    def __init__(self):
        self.spark = get_spark_for_loader()

    def load_to_iceberg(self, source: str, table: str, raw_path: str):
        """
        Main loading function called by Kafka consumer.
        """

        print(f"[LOADER] Reading raw parquet from: {raw_path}")
        df = self.spark.read.parquet(raw_path)

        iceberg_table = f"iceberg.default.{table}"
        print(f"[LOADER] Writing into Iceberg table: {iceberg_table}")

        (
            df.writeTo(iceberg_table)
            .createOrReplace()   # create if not exists, replace on next load
        )

        count = df.count()
        print(f"[LOADER] ✔ Successfully loaded {count} rows into {iceberg_table}")

        return count

    def stop(self):
        self.spark.stop()
