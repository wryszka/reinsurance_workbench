# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze (DLT) — the submission front door
# MAGIC
# MAGIC Ingests the GRLC/CDR-shaped landing (submissions MRC-slip header + premium/loss bordereaux + exposure),
# MAGIC applies DLT quality gates, and quarantines the seeded messy bordereau row. Also classifies the inbound
# MAGIC channel — clean structured **ADEPT/CDR** path vs the messy **MANUAL** path (mock feed only; no placement
# MAGIC network). Reads the landing Delta tables in batch (full-refresh each run — clean for the demo + reset).
# MAGIC
# MAGIC Module boundary: bronze tables are exposed through a stable, typed schema contract that silver consumes.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("source_catalog")
SCHEMA = spark.conf.get("source_schema")
LAND = f"{CATALOG}.{SCHEMA}"

VALID_STRUCTURES = ("Quota Share", "Surplus", "Cat XoL", "Risk XoL")
VALID_CHANNELS = ("ADEPT_CDR", "MANUAL")
VALID_PERILS = ("Windstorm", "Flood", "Motor", "Liability", "Hurricane", "GL", "Property", "Marine", "Engineering")

# COMMAND ----------

# MAGIC %md ## Submissions — MRC-slip header (treaty/fac, layers, RoL, channel)

# COMMAND ----------

@dlt.table(
    name="bronze_submissions",
    comment="Governed bronze reinsurance submissions (MRC-slip header, typed + quality-gated). One row per submission.",
    table_properties={"quality": "bronze", "layer": "bronze"},
)
@dlt.expect("valid_submission_id", "submission_public_id RLIKE '^sub:'")
@dlt.expect_or_drop("valid_structure", f"structure IN {VALID_STRUCTURES}")
@dlt.expect("valid_channel", f"inbound_channel IN {VALID_CHANNELS}")
@dlt.expect("non_negative_premium", "subject_premium_eur >= 0")
def bronze_submissions():
    return (
        spark.read.table(f"{LAND}.landing_submissions")
        .withColumn("subject_premium_eur", F.col("subject_premium_eur").cast("long"))
        .withColumn("layer_limit_eur", F.col("layer_limit_eur").cast("long"))
        .withColumn("layer_attachment_eur", F.col("layer_attachment_eur").cast("long"))
        .withColumn("rol_pct", F.col("rol_pct").cast("double"))
        .withColumn("inception_date", F.col("inception_date").cast("date"))
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md ## Premium + loss bordereaux + exposure

# COMMAND ----------

@dlt.table(
    name="bronze_premium_bordereaux",
    comment="Bronze premium bordereaux by submission and year (GWP, risk count). Quality-gated.",
    table_properties={"quality": "bronze", "layer": "bronze"},
)
@dlt.expect("positive_gwp", "gwp_eur > 0")
def bronze_premium_bordereaux():
    return (
        spark.read.table(f"{LAND}.landing_premium_bordereaux")
        .withColumn("gwp_eur", F.col("gwp_eur").cast("long"))
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name="bronze_loss_bordereaux",
    comment="Bronze loss bordereaux by submission/loss-year/peril (incurred, paid, as-if factor). The messy "
            "manual row (null incurred / invalid peril code) is dropped here and captured in bronze_quarantine_loss.",
    table_properties={"quality": "bronze", "layer": "bronze"},
)
@dlt.expect_or_drop("non_null_incurred", "incurred_eur IS NOT NULL")
@dlt.expect_or_drop("valid_peril", f"peril IN {VALID_PERILS}")
def bronze_loss_bordereaux():
    return (
        spark.read.table(f"{LAND}.landing_loss_bordereaux")
        .withColumn("incurred_eur", F.col("incurred_eur").cast("long"))
        .withColumn("paid_eur", F.col("paid_eur").cast("long"))
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name="bronze_exposure",
    comment="Bronze cedant exposure by submission/region/peril (TIV, location count, peak zone).",
    table_properties={"quality": "bronze", "layer": "bronze"},
)
@dlt.expect("positive_tiv", "tiv_eur > 0")
def bronze_exposure():
    return (
        spark.read.table(f"{LAND}.landing_exposure")
        .withColumn("tiv_eur", F.col("tiv_eur").cast("long"))
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md ## Quarantine — every row dropped by the loss-bordereaux gates (read from landing, the source)

# COMMAND ----------

@dlt.table(
    name="bronze_quarantine_loss",
    comment="Loss-bordereaux rows quarantined by the bronze quality gates (null incurred or invalid peril code). "
            "Read directly from landing so nothing is silently lost. Surfaces in the Intake 'messy manual path' view.",
    table_properties={"layer": "bronze"},
)
def bronze_quarantine_loss():
    src = spark.read.table(f"{LAND}.landing_loss_bordereaux")
    return (
        src.filter(f"incurred_eur IS NULL OR peril NOT IN {VALID_PERILS}")
        .withColumn(
            "quarantine_reason",
            F.when(F.col("incurred_eur").isNull(), F.lit("null_incurred"))
             .otherwise(F.lit("invalid_peril_code")),
        )
        .withColumn("_quarantined_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md ## ADEPT/CDR inbound-channel audit — clean structured vs messy manual (mock feed)

# COMMAND ----------

@dlt.table(
    name="bronze_inbound_audit",
    comment="Per-submission inbound-channel audit: ADEPT_CDR (clean structured ACORD GRLC-shaped feed) vs "
            "MANUAL (messy email/spreadsheet path), with slip completeness. Drives the Intake connectivity story.",
    table_properties={"layer": "bronze"},
)
def bronze_inbound_audit():
    return (
        spark.read.table(f"{LAND}.landing_submissions")
        .select(
            "submission_public_id", "cedant_id", "broker", "inbound_channel", "slip_completeness",
            F.when(F.col("inbound_channel") == "ADEPT_CDR", F.lit("structured"))
             .otherwise(F.lit("manual")).alias("path"),
            F.when(F.col("slip_completeness") >= 0.95, F.lit("complete"))
             .when(F.col("slip_completeness") >= 0.8, F.lit("minor_gaps"))
             .otherwise(F.lit("incomplete")).alias("completeness_band"),
        )
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )
