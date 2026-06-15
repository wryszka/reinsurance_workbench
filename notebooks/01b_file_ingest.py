# Databricks notebook source
# MAGIC %md
# MAGIC # 01b · Bronze (DLT) — the hard inbound formats
# MAGIC
# MAGIC Ingests the difficult data types with quality gates so they show up in the DLT event-log scorecard:
# MAGIC - **bronze_mrc_submissions** — Document-AI-extracted slips (from `landing_mrc_extractions`), gated on
# MAGIC   extraction confidence + structure validity; low-confidence rows → `bronze_quarantine_mrc`.
# MAGIC - **bronze_bordereau_files** — loss bordereaux CSVs read with an expected schema + `rescuedDataColumn`,
# MAGIC   so a cedant's schema-drifted file lands its renamed/extra columns in `_rescued_data` (quarantined).
# MAGIC - **bronze_event_footprint** — the semi-structured cat-event footprint JSON feed.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("source_catalog"); SCHEMA = spark.conf.get("source_schema")
REF = f"{CATALOG}.{SCHEMA}"
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
VALID_STRUCTURES = ("Quota Share", "Surplus", "Cat XoL", "Risk XoL")

# COMMAND ----------

# MAGIC %md ## Document-AI MRC extractions → gated bronze + quarantine

# COMMAND ----------

@dlt.table(name="bronze_mrc_submissions",
           comment="Structured fields extracted from unstructured MRC slips by Document AI (ai_query). Gated on "
                   "extraction confidence and structure validity; low-confidence extractions are quarantined.",
           table_properties={"quality": "bronze", "layer": "bronze"})
@dlt.expect_or_drop("confident_extraction", "extraction_confidence >= 0.75")
@dlt.expect("valid_structure", f"structure IN {VALID_STRUCTURES}")
@dlt.expect("layer_consistent", "layer_limit_eur IS NULL OR layer_attachment_eur IS NULL OR layer_limit_eur >= layer_attachment_eur")
def bronze_mrc_submissions():
    return spark.read.table(f"{REF}.landing_mrc_extractions").withColumn("_bronze_ingested_at", F.current_timestamp())


@dlt.table(name="bronze_quarantine_mrc",
           comment="MRC extractions held back by Document-AI confidence/validity gates (re-key or review).",
           table_properties={"layer": "bronze"})
def bronze_quarantine_mrc():
    src = spark.read.table(f"{REF}.landing_mrc_extractions")
    return (src.filter(f"extraction_confidence < 0.75 OR structure NOT IN {VALID_STRUCTURES} OR structure IS NULL")
            .withColumn("quarantine_reason",
                        F.when(F.col("extraction_confidence") < 0.75, F.lit("low_extraction_confidence"))
                         .otherwise(F.lit("invalid_or_missing_structure")))
            .withColumn("_quarantined_at", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md ## Schema-drifted bordereaux CSVs — read_files with rescuedDataColumn

# COMMAND ----------

@dlt.table(name="bronze_bordereau_files",
           comment="Loss-bordereaux CSV files read against the expected schema with rescuedDataColumn. A cedant's "
                   "schema-drifted file (renamed/extra columns) lands those values in _rescued_data — nothing is lost.",
           table_properties={"quality": "bronze", "layer": "bronze"})
@dlt.expect("known_schema", "_rescued_data IS NULL")
def bronze_bordereau_files():
    return (spark.read
            .schema("submission_public_id string, loss_year int, peril string, incurred_eur long")
            .option("header", "true").option("rescuedDataColumn", "_rescued_data")
            .csv(f"{BASE}/bordereaux_files/")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_bronze_ingested_at", F.current_timestamp()))


@dlt.table(name="bronze_quarantine_bordereaux",
           comment="Bordereaux rows whose schema drifted from the expected contract (captured in _rescued_data).",
           table_properties={"layer": "bronze"})
def bronze_quarantine_bordereaux():
    return (spark.read
            .schema("submission_public_id string, loss_year int, peril string, incurred_eur long")
            .option("header", "true").option("rescuedDataColumn", "_rescued_data")
            .csv(f"{BASE}/bordereaux_files/")
            .filter("_rescued_data IS NOT NULL")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("quarantine_reason", F.lit("schema_drift"))
            .withColumn("_quarantined_at", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md ## Cat-event footprint (semi-structured JSON feed)

# COMMAND ----------

@dlt.table(name="bronze_event_footprint",
           comment="Vendor catastrophe-event footprint feed (semi-structured JSON): event, intensity and the "
                   "affected CRESTA zones. Read with read_files / JSON inference.",
           table_properties={"quality": "bronze", "layer": "bronze"})
@dlt.expect("has_event_id", "event_public_id IS NOT NULL")
def bronze_event_footprint():
    return (spark.read.option("multiLine", "true").json(f"{BASE}/event_feed/")
            .withColumn("n_affected_cresta", F.size("affected_cresta"))
            .withColumn("_bronze_ingested_at", F.current_timestamp()))
