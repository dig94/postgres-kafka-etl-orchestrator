# jdbc_partitioning.py
from typing import List, Tuple, Optional


NUMERIC_LIKE = {
    "smallint", "integer", "bigint", "decimal", "numeric",
    "real", "double precision"
}
TIME_LIKE = {
    "date", "timestamp without time zone", "timestamp with time zone"
}


def get_column_type(spark, jdbc_url: str, props: dict, table: str, column: str, schema: str = "public") -> str:
    """
    Return DB-native data_type from information_schema.columns. Works with Postgres and most ANSI databases
    that expose information_schema (adjust schema filter per DB if needed). [web:153][web:156]
    """
    q = (
        f"(SELECT data_type FROM information_schema.columns "
        f"WHERE table_schema='{schema}' AND table_name='{table}' AND column_name='{column}') AS t"
    )
    df = spark.read.jdbc(jdbc_url, q, properties=props)
    rows = df.collect()
    if not rows:
        raise ValueError(f"Column {schema}.{table}.{column} not found in information_schema")
    return rows[0][0]  # e.g., 'uuid', 'bigint', 'timestamp without time zone' [web:153][web:156]


def is_range_partitionable(data_type: str) -> bool:
    """
    True when MIN/MAX range partitioning makes sense (numeric or timestamp). [web:65][web:155]
    """
    return (data_type in NUMERIC_LIKE) or (data_type in TIME_LIKE)  # [web:65][web:155]


def get_bounds_numeric_or_time(spark, jdbc_url: str, props: dict, table: str, column: str) -> Tuple[Optional[object], Optional[object]]:
    """
    Compute MIN/MAX for numeric/timestamp partition columns. Returns (lo, hi) or (None, None) if empty. [web:65][web:155]
    """
    query = f"(SELECT MIN({column}) AS lo, MAX({column}) AS hi FROM {table}) AS bounds"
    df = spark.read.jdbc(jdbc_url, query, properties=props)
    row = df.collect()[0]
    return row["lo"], row["hi"]  # None if table empty [web:65][web:155]


def read_range_partitioned(spark, jdbc_url: str, table: str, column: str, lo, hi, num_parts: int, props: dict):
    """
    Use Spark JDBC range partitioning documented options: column, lowerBound, upperBound, numPartitions. [web:65][web:155]
    """
    return spark.read.jdbc(
        url=jdbc_url,
        table=table,
        column=column,
        lowerBound=lo,
        upperBound=hi,
        numPartitions=num_parts,
        properties=props,
    )  # [web:65][web:155]


def build_uuid_predicates(db: str, column: str, num_parts: int) -> List[str]:
    """
    Build disjoint predicates for non-numeric keys like UUID or TEXT.
    """
    db = db.lower()
    if db == "postgres":
        # 🟢 Use this reliable, version-agnostic MD5 approach for Postgres/UUIDs
        # It takes MD5, converts the first 8 hex digits to an INT (via bit(32)),
        # and then takes the modulo. This avoids the non-standard hash() function.
        return [
            f"MOD(ABS((('x' || SUBSTR(MD5({column}::text), 1, 8))::bit(32))::int), {num_parts}) = {i}"
            for i in range(num_parts)
        ]
        
    if db in ("mysql", "mariadb"):
        return [f"MOD(CRC32({column}), {num_parts}) = {i}" for i in range(num_parts)]
    if db in ("sqlserver", "mssql"):
        return [f"ABS(CHECKSUM({column})) % {num_parts} = {i}" for i in range(num_parts)]
        
    # Fallback to hex-prefix split for other DBs if no specific hash is available
    hexes = list("0123456789abcdef")
    step = max(1, len(hexes) // max(1, num_parts))
    selected = hexes[::step][:num_parts]
    return [f"CAST({column} AS TEXT) LIKE '{h}%'" for h in selected]



def read_predicates_partitioned(spark, jdbc_url: str, table: str, predicates: List[str], props: dict):
    """
    Use Spark JDBC 'predicates' overload for parallel reads without numeric bounds. [web:57][web:150]
    """
    return spark.read.jdbc(
        url=jdbc_url,
        table=table,
        predicates=predicates,
        properties=props
    )  # [web:57][web:150]


def adaptive_read(
    spark,
    jdbc_url: str,
    db_flavor: str,
    table: str,
    key_column: str,
    props: dict,
    partitions: int = 8,
    schema: str = "public",
):
    """
    Decide partitioning strategy automatically and return a DataFrame.
    - If key is numeric/timestamp: range partitioning with bounds. [web:65][web:155]
    - Else: predicate-based partitioning using hash/CRC32/Checksum modulo. [web:151][web:150]
    """
    dt = get_column_type(spark, jdbc_url, props, table, key_column, schema=schema)  # [web:153][web:156]
    if is_range_partitionable(dt):
        lo, hi = get_bounds_numeric_or_time(spark, jdbc_url, props, table, key_column)  # [web:65][web:155]
        if lo is None or hi is None or lo == hi:
            # Single-partition read for empty or constant range. [web:65][web:155]
            return spark.read.jdbc(jdbc_url, table, properties=props)  # [web:65]
        # For numerics, scale partitions by range; for timestamps keep requested partitions. [web:65][web:155]
        if isinstance(lo, (int, float)) and isinstance(hi, (int, float)):
            total = float(hi) - float(lo)
            num_parts = 1 if total < 10000 else min(partitions, max(1, int(total // 1_000_000) + 1))  # [web:65]
        else:
            num_parts = partitions  # timestamps/dates [web:155]
        return read_range_partitioned(spark, jdbc_url, table, key_column, lo, hi, num_parts, props)  # [web:65][web:155]
    else:
        preds = build_uuid_predicates(db_flavor, key_column, max(1, partitions))  # [web:151]
        return read_predicates_partitioned(spark, jdbc_url, table, preds, props)  # [web:57][web:150]
