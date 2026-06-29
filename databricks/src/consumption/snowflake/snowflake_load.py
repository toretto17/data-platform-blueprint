"""
================================================================================
SNOWFLAKE LOAD — Consumption → Snowflake  [Databricks + Spark Snowflake connector]
================================================================================
Purpose: Twin of aws/src/consumption/snowflake/snowflake_load.py. Same Spark
         Snowflake connector; credentials from a Databricks secret scope.

Connector: Databricks includes the Snowflake connector on most runtimes. If not,
install the `spark-snowflake` + `snowflake-jdbc` libraries on the cluster.

Write modes: overwrite | append | merge (staged upsert).
Customize (CHANGE_ME): secret_scope, table, SF_* options, keys.

Platform notes: DBR 15.x LTS+. Same connector API as the AWS twin.
Version : 2026-06-28
================================================================================
"""
import logging

from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("snowflake_load_databricks")
spark = SparkSession.builder.getOrCreate()

SF_FORMAT = "net.snowflake.spark.snowflake"


class SnowflakeLoadDatabricks:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.table = cfg["table"]
        self.mode = cfg.get("mode", "overwrite")
        self.keys = cfg.get("keys", [])
        self.sf_options = self._build_options()

    def _secret(self, scope: str, key: str) -> str:
        return dbutils.secrets.get(scope, key)  # noqa: F821 (dbutils injected in Databricks)

    def _build_options(self) -> dict:
        scope = self.cfg["secret_scope"]                    # CHANGE_ME
        return {
            "sfURL": self._secret(scope, "sfURL"),
            "sfUser": self._secret(scope, "sfUser"),
            "sfPassword": self._secret(scope, "sfPassword"),
            "sfDatabase": self.cfg.get("sf_database", "ANALYTICS"),
            "sfSchema": self.cfg.get("sf_schema", "PUBLIC"),
            "sfWarehouse": self.cfg.get("sf_warehouse", "COMPUTE_WH"),
            "sfRole": self.cfg.get("sf_role", ""),
        }

    def write(self, df: DataFrame):
        if self.mode in ("overwrite", "append"):
            (df.write.format(SF_FORMAT).options(**self.sf_options)
               .option("dbtable", self.table).mode(self.mode).save())
            logger.info(f"[{self.mode}] wrote → Snowflake {self.table}")
        elif self.mode == "merge":
            self._merge(df)
        else:
            raise ValueError("mode must be overwrite|append|merge")

    def _merge(self, df: DataFrame):
        if not self.keys:
            raise ValueError("merge mode requires keys")
        stage = f"{self.table}__stage"
        (df.write.format(SF_FORMAT).options(**self.sf_options)
           .option("dbtable", stage).mode("overwrite").save())
        on = " AND ".join([f"t.{k} = s.{k}" for k in self.keys])
        set_cols = ", ".join([f"t.{c} = s.{c}" for c in df.columns])
        cols = ", ".join(df.columns)
        vals = ", ".join([f"s.{c}" for c in df.columns])
        merge_sql = f"""
            MERGE INTO {self.table} t USING {stage} s ON {on}
            WHEN MATCHED THEN UPDATE SET {set_cols}
            WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals});
        """
        sc = spark.sparkContext
        sf_utils = sc._jvm.net.snowflake.spark.snowflake.Utils
        jmap = sc._jvm.java.util.HashMap()
        for k, v in self.sf_options.items():
            jmap.put(k, v)
        sf_utils.runQuery(jmap, merge_sql)
        sf_utils.runQuery(jmap, f"DROP TABLE IF EXISTS {stage}")
        logger.info(f"[merge] upserted → Snowflake {self.table} on {self.keys}")

    def run(self, df: DataFrame):
        self.write(df)


if __name__ == "__main__":
    cfg = {"secret_scope": "snowflake", "table": "ANALYTICS.PUBLIC.SALES_MART",  # CHANGE_ME
           "mode": "overwrite"}
    df = spark.table("main.consumption.sales_mart")   # CHANGE_ME
    SnowflakeLoadDatabricks(cfg).run(df)
