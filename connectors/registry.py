from connectors.postgres import PostgresConnector
from connectors.my_sql import MySQLConnector
# add more as you build them (Mongo, Cassandra, S3...)

class ConnectorRegistry:
    CONNECTORS = {
        "postgres": PostgresConnector,
        "mysql": MySQLConnector,
        # "mongodb": MongoDBConnector,
        # "cassandra": CassandraConnector,
        # "s3": S3Connector,
    }

    @staticmethod
    def get_connector(conn_type: str, **kwargs):
        """
        Returns a connector instance for the given type.
        Example: get_connector("postgres", host="...", db="...")
        """
        conn_type = conn_type.lower()
        if conn_type not in ConnectorRegistry.CONNECTORS:
            raise ValueError(f"❌ Unsupported connector type: {conn_type}")

        connector_cls = ConnectorRegistry.CONNECTORS[conn_type]
        return connector_cls(**kwargs)
