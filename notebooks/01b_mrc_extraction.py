# Databricks notebook source
# MAGIC %md
# MAGIC # 01b · Document AI — extract structured fields from MRC submission slips
# MAGIC
# MAGIC The genuinely-hard data type: unstructured ACORD-shaped submission slips (text in a UC Volume) → structured
# MAGIC fields via **`ai_query`** against the Foundation Model API (Claude). Today reinsurers re-key these by hand.
# MAGIC We compute an **extraction confidence** (field completeness) so the bronze layer can quality-gate and
# MAGIC quarantine low-confidence extractions. `ai_query` + `read_files` are GA SQL on serverless.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
dbutils.widgets.text("fm_endpoint", "databricks-claude-sonnet-4-6")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema"); FM = dbutils.widgets.get("fm_endpoint")
fqn = f"{catalog}.{schema}"
from pyspark.sql import functions as F, types as T
BASE = f"/Volumes/{catalog}/{schema}/landing/mrc_slips"

PROMPT = ("You are an ACORD reinsurance submission parser. From the Market Reform Contract slip below, return ONLY "
          "a JSON object with these keys: submission_public_id (e.g. 'sub:900002'), cedant, broker, structure "
          "(one of Quota Share, Surplus, Cat XoL, Risk XoL), proportional_or_xol (Proportional or XoL), lob, "
          "territories, perils, layer_limit_eur (integer, no symbols/commas, null if proportional), "
          "layer_attachment_eur (integer or null), subject_premium_eur (integer), rol_pct (decimal fraction e.g. "
          "0.182, null if not stated). Use null for anything not present. Return only the JSON.\\n\\nSLIP:\\n")

# COMMAND ----------

# ai_query over each slip file (one row per document via wholeText). Escape single quotes for the SQL literal.
PROMPT_SQL = PROMPT.replace("'", "''")
raw = spark.sql(f"""
  SELECT _metadata.file_path AS source_file, value AS slip_text,
         ai_query('{FM}', CONCAT('{PROMPT_SQL}', value)) AS extracted_json
  FROM read_files('{BASE}/', format => 'text', wholeText => true)
""")

schema_json = T.StructType([
    T.StructField("submission_public_id", T.StringType()), T.StructField("cedant", T.StringType()),
    T.StructField("broker", T.StringType()), T.StructField("structure", T.StringType()),
    T.StructField("proportional_or_xol", T.StringType()), T.StructField("lob", T.StringType()),
    T.StructField("territories", T.StringType()), T.StructField("perils", T.StringType()),
    T.StructField("layer_limit_eur", T.LongType()), T.StructField("layer_attachment_eur", T.LongType()),
    T.StructField("subject_premium_eur", T.LongType()), T.StructField("rol_pct", T.DoubleType())])

# strip any markdown fences, parse JSON
cleaned = raw.withColumn("j", F.regexp_replace(F.col("extracted_json"), "```json|```", "")) \
             .withColumn("e", F.from_json("j", schema_json))
KEY = ["cedant", "broker", "structure", "proportional_or_xol", "lob", "territories", "perils", "subject_premium_eur", "rol_pct"]
conf = sum([F.when(F.col(f"e.{k}").isNotNull(), F.lit(1.0)).otherwise(F.lit(0.0)) for k in KEY]) / F.lit(len(KEY))

out = cleaned.select(
    F.coalesce(F.col("e.submission_public_id"),
               F.regexp_replace(F.regexp_extract("source_file", "(sub_[0-9]+)", 1), "_", ":")).alias("submission_public_id"),
    "e.cedant", "e.broker", "e.structure", "e.proportional_or_xol", "e.lob", "e.territories", "e.perils",
    F.col("e.layer_limit_eur").alias("layer_limit_eur"), F.col("e.layer_attachment_eur").alias("layer_attachment_eur"),
    F.col("e.subject_premium_eur").alias("subject_premium_eur"), F.col("e.rol_pct").alias("rol_pct"),
    F.round(conf, 2).alias("extraction_confidence"),
    F.col("source_file"), F.substring("slip_text", 1, 1200).alias("slip_excerpt")
).withColumn("_extracted_at", F.current_timestamp())

out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.landing_mrc_extractions")
print(f"extracted {out.count()} slips")
out.select("submission_public_id", "cedant", "structure", "layer_limit_eur", "rol_pct", "extraction_confidence").show(truncate=False)
