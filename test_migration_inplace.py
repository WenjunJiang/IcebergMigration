#!/usr/bin/env python3
import argparse
import os
from pyspark.sql import SparkSession, functions as F

DEFAULT_ICEBERG_VER = "1.9.2"
DEFAULT_SQLITE_JDBC_VER = "3.50.3.0"

def to_file_uri(path_or_uri: str) -> str:
    """Normalize to file:// URI (accepts file:/..., file://..., or /abs/path)."""
    s = path_or_uri.strip()
    if s.startswith("file:/"):
        return s
    if os.path.isabs(s):
        return "file://" + os.path.abspath(s)
    # if it's a relative path, make it absolute
    return "file://" + os.path.abspath(s)

def main():
    p = argparse.ArgumentParser(
        description="Verify Iceberg table paths after migration (JDBC SQLite catalog)."
    )
    p.add_argument("SQLITE_DB", help="Path to SQLite DB (e.g. /.../iceberg.db)")
    p.add_argument("WAREHOUSE", help="Warehouse root (e.g. file:///.../warehouse or /.../warehouse)")
    p.add_argument("TABLE", help="Table identifier (e.g. local.my_table)")
    p.add_argument("--iceberg-ver", default=DEFAULT_ICEBERG_VER, help=f"Iceberg runtime version (default {DEFAULT_ICEBERG_VER})")
    p.add_argument("--sqlite-jdbc-ver", default=DEFAULT_SQLITE_JDBC_VER, help=f"SQLite JDBC version (default {DEFAULT_SQLITE_JDBC_VER})")
    p.add_argument("--sample", type=int, default=5, help="Number of file paths to sample (default 5)")
    args = p.parse_args()

    sqlite_db = os.path.abspath(args.SQLITE_DB)
    warehouse_uri = to_file_uri(args.WAREHOUSE)
    table = args.TABLE.strip()

    iceberg_ver = args.iceberg_ver
    sqlite_jdbc_ver = args.sqlite_jdbc_ver

    spark = (
        SparkSession.builder
        .appName("Iceberg-JDBC-SQLite-VerifyNoCALL")
        .config(
            "spark.jars.packages",
            f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{iceberg_ver},"
            f"org.xerial:sqlite-jdbc:{sqlite_jdbc_ver}"
        )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "jdbc")
        .config("spark.sql.catalog.ice.uri", f"jdbc:sqlite:{sqlite_db}")
        .config("spark.sql.catalog.ice.driver", "org.sqlite.JDBC")
        .config("spark.sql.catalog.ice.warehouse", warehouse_uri)
        .config("spark.sql.catalog.ice.jdbc.schema-version", "V1")
        .getOrCreate()
    )

    try:
        # 1) Row count
        spark.sql(f"SELECT COUNT(*) AS cnt FROM ice.{table}").show()

        # 2) Table location from DESCRIBE TABLE EXTENDED
        desc = spark.sql(f"DESCRIBE TABLE EXTENDED ice.{table}")
        loc_rows = desc.where(F.col("col_name") == "Location").select("data_type").collect()
        table_location = loc_rows[0][0] if loc_rows else "(unknown)"
        print(f"Table Location: {table_location}")

        # 3) Sample actual data files without CALL
        df = spark.read.table(f"ice.{table}")
        sample_paths = (
            df.select(F.input_file_name().alias("file_path"))
              .where(F.col("file_path").isNotNull())
              .limit(args.sample)
              .distinct()
              .collect()
        )

        print("Sample data file paths:")
        for r in sample_paths:
            print(" -", r["file_path"])

        # 4) Assert all sampled paths are under NEW warehouse
        prefix = warehouse_uri.rstrip("/") + "/"
        bad = [r["file_path"] for r in sample_paths if not r["file_path"].startswith(prefix)]
        if bad:
            raise RuntimeError(
                f"Some sampled paths are NOT under {prefix}:\n" + "\n".join(bad)
            )
        else:
            print(f"All sampled paths are under {prefix} ✅")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
