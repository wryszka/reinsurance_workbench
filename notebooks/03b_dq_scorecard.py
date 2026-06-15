# Databricks notebook source
# MAGIC %md
# MAGIC # 03b · Data-quality scorecard — from the real DLT event log
# MAGIC
# MAGIC Parses the medallion pipeline's **event log** (published to `medallion_event_log`) for the actual
# MAGIC expectation metrics (passed/failed records per rule) and builds `gold_dq_scorecard`. This is genuine —
# MAGIC the numbers come from DLT's own evaluation of every `@dlt.expect`, not a hand-written table.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"
from pyspark.sql import functions as F, Window

# Rule predicates (the event log carries names + counts, not the predicate text) — surfaced in the scorecard.
PREDICATE = {
    "valid_submission_id": "submission_public_id RLIKE '^sub:'", "valid_structure": "structure IN (QS, Surplus, Cat XoL, Risk XoL)",
    "valid_channel": "inbound_channel IN (ADEPT_CDR, MANUAL)", "non_negative_premium": "subject_premium_eur >= 0",
    "positive_gwp": "gwp_eur > 0", "non_null_incurred": "incurred_eur IS NOT NULL", "valid_peril": "peril IN (known perils)",
    "positive_tiv": "tiv_eur > 0", "has_cedant": "cedant_name IS NOT NULL", "known_zone": "zone_id IS NOT NULL",
    "confident_extraction": "extraction_confidence >= 0.75", "layer_consistent": "layer_limit_eur >= layer_attachment_eur",
    "known_schema": "_rescued_data IS NULL (no schema drift)", "has_event_id": "event_public_id IS NOT NULL",
}
LAYER = lambda t: ("bronze" if t.startswith("bronze") else "silver" if t.startswith("silver") else "gold")

# COMMAND ----------

el = spark.table(f"{fqn}.medallion_event_log")
exp_schema = "array<struct<name string, dataset string, passed_records bigint, failed_records bigint>>"
rows = (el.filter("event_type = 'flow_progress'")
        .where(F.expr("details:flow_progress.data_quality.expectations IS NOT NULL"))
        .select(F.col("timestamp"),
                F.explode(F.from_json(F.expr("details:flow_progress.data_quality.expectations"), exp_schema)).alias("e")))
# keep the latest observation per (dataset, expectation)
w = Window.partitionBy("e.dataset", "e.name").orderBy(F.col("timestamp").desc())
latest = (rows.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
          .select(F.element_at(F.split(F.col("e.dataset"), "\\."), -1).alias("table_name"), F.col("e.name").alias("expectation"),
                  F.col("e.passed_records").alias("passing_records"), F.col("e.failed_records").alias("failing_records")))

@F.udf("string")
def _pred(name): return PREDICATE.get(name, name)
@F.udf("string")
def _layer(t): return LAYER(t or "")

scorecard = (latest
             .withColumn("total_records", F.col("passing_records") + F.col("failing_records"))
             .withColumn("pass_rate_pct", F.round(F.col("passing_records") * 100.0 / F.greatest(F.col("total_records"), F.lit(1)), 1))
             .withColumn("predicate", _pred(F.col("expectation")))
             .withColumn("layer", _layer(F.col("table_name")))
             .withColumn("_built_at", F.current_timestamp()))
scorecard.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.gold_dq_scorecard")
n = spark.table(f"{fqn}.gold_dq_scorecard").count()
print(f"gold_dq_scorecard: {n} expectations across the pipeline")
scorecard.orderBy("table_name", "expectation").show(50, truncate=False)
