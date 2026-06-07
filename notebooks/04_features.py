# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Feature engineering (UC Feature Store)
# MAGIC
# MAGIC Builds `feature_submission` — the triage + pricing feature vector keyed by `submission_public_id`,
# MAGIC registered with the Feature Engineering client. This is the feature contract the P5 models and the
# MAGIC crux consume via `FeatureLookup`. Reads only the silver contract + the as-at accumulation mart.
# MAGIC
# MAGIC Module boundary: features are an independent module; models look them up by key, never recompute them.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

from pyspark.sql import functions as F
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# COMMAND ----------

# MAGIC %md ## Assemble the feature vector from the silver contract

# COMMAND ----------

subs = spark.table(f"{fqn}.silver_submissions")
acc = spark.table(f"{fqn}.inforce_accumulation").select(
    "zone_id",
    F.col("utilisation_pct").alias("zone_utilisation_pct"),
    F.col("headroom_eur").alias("zone_headroom_eur"))

feat = (subs
        .join(acc, "zone_id", "left")
        .select(
            "submission_public_id",
            F.col("subject_premium_eur").cast("double").alias("subject_premium_eur"),
            F.coalesce(F.col("ceded_share_pct"), F.lit(0.0)).alias("ceded_share_pct"),
            F.coalesce(F.col("rol_pct"), F.lit(0.0)).alias("rol_pct"),
            F.coalesce(F.col("as_if_loss_ratio"), F.lit(0.5)).alias("as_if_loss_ratio"),
            F.coalesce(F.col("large_loss_count"), F.lit(0)).cast("int").alias("large_loss_count"),
            F.coalesce(F.col("total_tiv_eur"), F.lit(0)).cast("double").alias("total_tiv_eur"),
            F.coalesce(F.col("data_quality_score"), F.lit(0.9)).alias("data_quality_score"),
            F.coalesce(F.col("rate_adequacy"), F.lit(1.0)).alias("rate_adequacy"),
            F.col("is_cat_xol").cast("int").alias("is_cat_xol"),
            F.coalesce(F.col("credit_quality_step"), F.lit(3)).cast("int").alias("credit_quality_step"),
            F.coalesce(F.col("one_year_pd_pct"), F.lit(0.1)).alias("counterparty_pd_pct"),
            F.col("is_peak").cast("int").alias("is_peak_zone"),
            F.coalesce(F.col("zone_utilisation_pct"), F.lit(0.0)).alias("zone_utilisation_pct"),
            F.coalesce(F.col("expected_loss_ratio"), F.lit(0.5)).alias("expected_loss_ratio"),
        ))

# COMMAND ----------

# MAGIC %md ## Register as a Feature Store table (PK = submission_public_id)

# COMMAND ----------

table_name = f"{fqn}.feature_submission"
existing = [t.name for t in spark.catalog.listTables(f"{catalog}.{schema}") if t.name == "feature_submission"]
if existing:
    fe.write_table(name=table_name, df=feat, mode="merge")
else:
    fe.create_table(
        name=table_name,
        primary_keys=["submission_public_id"],
        df=feat,
        description="Reinsurance submission feature vector (triage + pricing), keyed by submission_public_id.",
    )

print(f"feature_submission rows: {spark.table(table_name).count()}")
feat.filter("submission_public_id IN ('sub:900001','sub:900002')").show(truncate=False)
