from pyspark.sql import SparkSession

def get_spark_for_loader():
    spark = (
        SparkSession.builder
        .appName("IcebergLoader")

        # Iceberg catalog
        .config("spark.sql.catalog.minio", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.minio.type", "hadoop")
        .config("spark.sql.catalog.minio.warehouse", "s3a://warehouse/")

        # Iceberg extensions
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

        # MinIO (S3A) settings
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # Turn off SSL
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # S3 optimization
        .config("spark.hadoop.fs.s3a.fast.upload", "true")

        # Required for Java 11
        .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
        .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")

        # Improve Iceberg performance
        .config("spark.sql.catalog.minio.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.sql.shuffle.partitions", "4")
        
        .getOrCreate()
    )
    return spark


class LoaderService:
    """
    Converts raw MinIO Parquet → Iceberg tables.
    """

    def __init__(self):
        self.spark = get_spark_for_loader()

    def load_to_iceberg(self, source: str, table: str, raw_path: str):
        """
        Main loading function called by Kafka consumer.
        """

        print(f"[LOADER] Reading raw data from: {raw_path}")
        df = self.spark.read.parquet(raw_path)

        iceberg_table = f"minio.default.{table}"

        print(f"[LOADER] Writing into Iceberg table: {iceberg_table}")

        (
            df.writeTo(iceberg_table)
            .createOrReplace()      # first time → create
            # .append()             # if you only want incremental loads
        )

        count = df.count()
        print(f"[LOADER] ✔ Loaded {count} rows into Iceberg table {iceberg_table}")

        return count

    def stop(self):
        self.spark.stop()
