#!/usr/bin/env python3
"""
End-to-End Cloud Data Pipeline — PySpark Version
Poojitha Krishna Jonnalagadda - Data Engineer (Python, SQL, Spark/PySpark)

Demonstration sample built from my data engineering work: moving SAP-style
master and transactional data (customers, materials, sales orders) through

    INGEST -> TRANSFORM/CLEANSE -> VALIDATE -> LOAD (partitioned) -> REPORT

This is the cloud form of the pipeline. The identical logic runs locally
with no dependencies via run_pipeline_local.py, which produces the real
output saved under output/.

REQUIREMENTS: Apache Spark / PySpark (pip install pyspark) or a managed
Spark environment - Amazon EMR, AWS Glue, Azure Databricks / Azure HDInsight,
or Databricks on AWS. For the Delta Lake write path, the delta-spark package
must be configured on the session; without it the script falls back to
partitioned Parquet, which Databricks/EMR/Glue all read natively.

Usage (local Spark):
    spark-submit pyspark_etl_sample.py [input_dir] [output_dir]
Defaults: input_dir = sample_input/, output_dir = output_spark/
On a cloud platform the same paths become s3://... or abfss://... locations.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_DIR = sys.argv[1] if len(sys.argv) > 1 else "sample_input"
OUTPUT_DIR = sys.argv[2] if len(sys.argv) > 2 else "output_spark"

spark = (SparkSession.builder
         .appName("sap-master-data-pipeline-sample")
         .getOrCreate())

# ---------------------------------------------------------------------------
# INGEST - land the raw extracts unchanged (bronze). In production these
# files land in S3 / Azure Data Lake from SAP or flat-file drops.
# ---------------------------------------------------------------------------
customers = spark.read.option("header", True).csv(f"{INPUT_DIR}/customers.csv")
materials = spark.read.option("header", True).csv(f"{INPUT_DIR}/materials.csv")
orders = spark.read.option("header", True).csv(f"{INPUT_DIR}/sales_orders.csv")

# ---------------------------------------------------------------------------
# TRANSFORM / CLEANSE - trim, normalise case on code fields, dedupe on the
# natural keys (silver). Same rules as the local runner.
# ---------------------------------------------------------------------------
def trim_all(df):
    return df.select([F.trim(F.col(c)).alias(c) for c in df.columns])


customers = (trim_all(customers)
             .withColumn("country", F.upper(F.col("country")))
             .dropDuplicates(["customer_id"]))
materials = (trim_all(materials)
             .withColumn("currency", F.upper(F.col("currency")))
             .dropDuplicates(["material_id", "plant"]))
orders = trim_all(orders)

# ---------------------------------------------------------------------------
# VALIDATE - build a reject flag per rule; rejected rows are counted and
# reported per rule, never silently dropped.
# ---------------------------------------------------------------------------
customers = customers.withColumn(
    "reject_reason",
    F.when(F.col("customer_id").isNull() | (F.col("customer_id") == "")
           | F.col("name").isNull() | (F.col("name") == "")
           | F.col("sales_org").isNull() | (F.col("sales_org") == ""),
           F.lit("mandatory field missing"))
     .when(~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
           F.lit("email missing or invalid"))
     .otherwise(F.lit(None)))

materials = materials.withColumn(
    "reject_reason",
    F.when(F.col("material_id").isNull() | (F.col("material_id") == "")
           | F.col("plant").isNull() | (F.col("plant") == "")
           | F.col("material_group").isNull() | (F.col("material_group") == ""),
           F.lit("mandatory field missing"))
     .when(F.col("unit_price").cast("double").isNull()
           | (F.col("unit_price").cast("double") <= 0),
           F.lit("unit_price not positive"))
     .otherwise(F.lit(None)))

valid_customer_ids = customers.filter(F.col("reject_reason").isNull()) \
                              .select("customer_id")
valid_material_ids = materials.filter(F.col("reject_reason").isNull()) \
                              .select("material_id")

orders = (orders
          .join(valid_customer_ids.withColumn("cust_ok", F.lit(True)),
                on="customer_id", how="left")
          .join(valid_material_ids.withColumn("mat_ok", F.lit(True)),
                on="material_id", how="left")
          .withColumn(
              "reject_reason",
              F.when(F.col("order_id").isNull() | (F.col("order_id") == ""),
                     F.lit("mandatory field missing"))
               .when(F.col("quantity").cast("int").isNull()
                     | (F.col("quantity").cast("int") <= 0),
                     F.lit("quantity not positive"))
               .when(F.col("cust_ok").isNull(),
                     F.lit("customer not in cleansed customers"))
               .when(F.col("mat_ok").isNull(),
                     F.lit("material not in cleansed materials"))
               .otherwise(F.lit(None)))
          .drop("cust_ok", "mat_ok"))

# ---------------------------------------------------------------------------
# LOAD - clean rows only, partitioned for downstream reporting (gold).
# Delta Lake when available, partitioned Parquet otherwise.
# ---------------------------------------------------------------------------
def write_partitioned(df, entity, partition_col):
    clean = df.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    path = f"{OUTPUT_DIR}/{entity}"
    try:
        (clean.write.mode("overwrite")
              .format("delta")
              .partitionBy(partition_col)
              .save(path))
        return "delta"
    except Exception:
        (clean.write.mode("overwrite")
              .format("parquet")
              .partitionBy(partition_col)
              .save(path))
        return "parquet"


cust_fmt = write_partitioned(customers, "customers", "sales_org")
mat_fmt = write_partitioned(materials, "materials", "plant")
ord_fmt = write_partitioned(orders, "sales_orders", "sales_org")

# ---------------------------------------------------------------------------
# REPORT - the same reconciliation shape as the local runner and as the
# load reports I attach to cutover sign-offs: input = rejected + clean.
# ---------------------------------------------------------------------------
for name, df in (("customers", customers), ("materials", materials),
                 ("sales_orders", orders)):
    total = df.count()
    rejected = df.filter(F.col("reject_reason").isNotNull()).count()
    print(f"{name}: {total} rows after dedupe -> {rejected} rejected, "
          f"{total - rejected} clean")
    (df.filter(F.col("reject_reason").isNotNull())
       .groupBy("reject_reason").count()
       .show(truncate=False))

print(f"Output written under {OUTPUT_DIR} "
      f"(customers={cust_fmt}, materials={mat_fmt}, sales_orders={ord_fmt})")

spark.stop()
