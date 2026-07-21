"""
hive_utils.py

Shared helper for registering a Delta (or other-format) table location as
an external Hive table, so any layer — bronze, silver, gold, or a
snapshot export — can be made queryable through the metastore the same
way.
"""

import airflow.scripts.config as config


def register_external_table(spark, table_name: str, location: str, fmt: str = "delta", database: str = None) -> None:
    """
    Registers (or refreshes) `location` as an external Hive table named
    `database.table_name`, stored in the given format.

    This is metadata-only — it does not move or copy any data files, it
    just points Hive's metastore at wherever the files already live.
    """
    database = database or config.HIVE_DATABASE
    full_name = f"{database}.{table_name}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    spark.sql(f"""
        CREATE EXTERNAL TABLE {full_name}
        USING {fmt}
        LOCATION '{location}'
    """)
    spark.sql(f"REFRESH TABLE {full_name}")

    print(f"[*] Registered Hive table: {full_name} -> {location} ({fmt})")