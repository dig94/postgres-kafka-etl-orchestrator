from connectors.base_connector import BaseConnector
from connectors.config import Config
from pyspark.sql import SparkSession

class MySQLConnector(BaseConnector):
    def __init__(self):

        self.host = Config.MYSQL_HOST
        self.port = Config.MYSQL_PORT
        self.db = Config.MYSQL_DB
        self.user = Config.MYSQL_USER
        self.password = Config.MYSQL_PASSWORD
        self.jdbc_url = f"jdbc:mysql://{self.host}:{self.port}/{self.db}?useSSL=false"

        self.spark = (
            SparkSession.builder
            .appName("MySQLJDBCExtractor")
            .config("spark.jars", "mysql-connector-java-8.0.13.jar")
            .getOrCreate()
        )

        self.props = {"user": self.user, "password": self.password, "driver": "com.mysql.cj.jdbc.Driver"}

    def list_entities(self):
        query = "(SELECT table_name FROM information_schema.tables WHERE table_schema=DATABASE()) as t"
        df = self.spark.read.jdbc(self.jdbc_url, query, properties=self.props)
        return [r["table_name"] for r in df.collect()]

    def get_bounds(self, table, key_column):
        query = f"(SELECT MIN({key_column}) as min_id, MAX({key_column}) as max_id FROM {table}) as b"
        df = self.spark.read.jdbc(self.jdbc_url, query, properties=self.props)
        row = df.collect()[0]
        return int(row["min_id"] or 0), int(row["max_id"] or 0)

    def read_range(self, table, key_column, start, end):
        query = f"(SELECT * FROM {table} WHERE {key_column} >= {start} AND {key_column} < {end}) as chunk"
        return self.spark.read.jdbc(self.jdbc_url, query, properties=self.props)

    def extract_data(self, entity, key_column="id", partitions=8):
        lo, hi = self.get_bounds(entity, key_column)
        if lo == hi:
            return self.spark.read.jdbc(self.jdbc_url, entity, properties=self.props)

        total = hi - lo
        num_parts = 1 if total < 10000 else min(partitions, total // 1000000 + 1)

        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table=entity,
            column=key_column,
            lowerBound=lo,
            upperBound=hi,
            numPartitions=num_parts,
            properties=self.props
        )
