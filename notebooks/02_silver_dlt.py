# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver (DLT) — structure & enrich
# MAGIC
# MAGIC Joins the bronze front door with cedant/counterparty reference, peak-zone metadata, loss history and
# MAGIC exposure into clean typed entities. `silver_submissions` is the stable input contract for the feature
# MAGIC store (P4) and the crux (P5) — one row per `submission_public_id`, everything a scorer needs pre-joined.
# MAGIC
# MAGIC Module boundary: downstream layers depend only on the documented silver column contract, never on bronze.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("source_catalog")
SCHEMA = spark.conf.get("source_schema")
REF = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

# MAGIC %md ## Per-submission rollups from the bordereaux

# COMMAND ----------

@dlt.table(
    name="silver_loss_summary",
    comment="Per-submission loss-history rollup: 5y as-if loss ratio, large-loss count, worst-year ratio.",
    table_properties={"quality": "silver", "layer": "silver"},
)
def silver_loss_summary():
    losses = dlt.read("bronze_loss_bordereaux")
    prem = dlt.read("bronze_premium_bordereaux").groupBy("submission_public_id").agg(
        F.avg("gwp_eur").alias("avg_gwp_eur"))
    agg = (losses
           .withColumn("as_if_incurred_eur", (F.col("incurred_eur") * F.col("as_if_factor")).cast("long"))
           .groupBy("submission_public_id").agg(
               F.sum("as_if_incurred_eur").alias("total_as_if_incurred_eur"),
               F.avg("as_if_incurred_eur").alias("avg_annual_incurred_eur"),
               F.sum(F.col("large_loss_flag").cast("int")).alias("large_loss_count"),
               F.sum("n_claims").alias("total_claims"),
               F.count("*").alias("loss_years")))
    return (agg.join(prem, "submission_public_id", "left")
            .withColumn("as_if_loss_ratio",
                        F.round(F.col("avg_annual_incurred_eur") / F.col("avg_gwp_eur"), 4))
            .withColumn("_silver_built_at", F.current_timestamp()))


@dlt.table(
    name="silver_exposure_summary",
    comment="Per-submission exposure rollup: total TIV, location count, peak zone.",
    table_properties={"quality": "silver", "layer": "silver"},
)
def silver_exposure_summary():
    return (dlt.read("bronze_exposure")
            .groupBy("submission_public_id", "zone_id").agg(
                F.sum("tiv_eur").alias("total_tiv_eur"),
                F.sum("n_locations").alias("total_locations"))
            .withColumn("_silver_built_at", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md ## silver_submissions — the core enriched entity (one row per submission)

# COMMAND ----------

@dlt.table(
    name="silver_submissions",
    comment="The clean, enriched submission entity — one row per submission_public_id with cedant + counterparty "
            "credit quality, peak-zone metadata, loss-history rollup, exposure and data-quality. Stable input "
            "contract for the feature store and the accumulation/capital crux.",
    table_properties={"quality": "silver", "layer": "silver"},
)
@dlt.expect("has_cedant", "cedant_name IS NOT NULL")
@dlt.expect("known_zone", "zone_id IS NOT NULL")
def silver_submissions():
    subs = dlt.read("bronze_submissions")
    cedants = spark.read.table(f"{REF}.ref_cedants")
    cps = spark.read.table(f"{REF}.counterparties").select(
        "cedant_id", "one_year_pd_pct", "outlook")
    zones = spark.read.table(f"{REF}.ref_peak_zones").select(
        "zone_id", "zone_name", "peril", "region", "is_peak", "appetite_pml_1in200_eur")
    loss = dlt.read("silver_loss_summary").select(
        "submission_public_id", "as_if_loss_ratio", "large_loss_count", "total_claims", "loss_years")
    expo = dlt.read("silver_exposure_summary").select(
        "submission_public_id", "total_tiv_eur", "total_locations")
    # Denormalise the as-at zone accumulation + correlated-treaty info onto the submission, so the crux UC
    # functions are a single-table, single-row lookup (a SQL scalar UDF body cannot contain a decorrelatable
    # JOIN+GROUP BY). zone_current_pml + n_correlated + correlated_treaty_ids live here.
    acc = spark.read.table(f"{REF}.inforce_accumulation").select(
        "zone_id", F.col("current_pml_1in200_eur").alias("zone_current_pml_1in200_eur"))
    corr = (spark.read.table(f"{REF}.inforce_treaties")
            .filter("structure = 'Cat XoL' AND is_correlated_ref = true")
            .groupBy("zone_id").agg(F.count("*").cast("int").alias("n_correlated"),
                                    F.sort_array(F.collect_list("treaty_id")).alias("correlated_treaty_ids")))

    return (subs
            .join(cedants, "cedant_id", "left")
            .join(cps, "cedant_id", "left")
            .join(zones, "zone_id", "left")
            .join(loss, "submission_public_id", "left")
            .join(expo, "submission_public_id", "left")
            .join(acc, "zone_id", "left")
            .join(corr, "zone_id", "left")
            .withColumn("n_correlated", F.coalesce(F.col("n_correlated"), F.lit(0)))
            .withColumn("correlated_treaty_ids", F.coalesce(F.col("correlated_treaty_ids"), F.array()))
            # data-quality score: slip completeness adjusted for manual-path penalty
            .withColumn("data_quality_score",
                        F.round(F.col("slip_completeness") *
                                F.when(F.col("inbound_channel") == "ADEPT_CDR", F.lit(1.0)).otherwise(F.lit(0.92)), 3))
            # rate adequacy proxy: technical RoL needed vs offered (for XoL); for proportional use loss ratio headroom
            .withColumn("is_cat_xol", (F.col("proportional_or_xol") == "XoL").cast("int"))
            .withColumn("rate_adequacy",
                        F.when(F.col("proportional_or_xol") == "XoL",
                               F.round(F.col("rol_pct") / (F.coalesce(F.col("as_if_loss_ratio"), F.lit(0.45)) * F.col("rol_pct") + 0.06), 3))
                         .otherwise(F.round((F.lit(1.0) - F.coalesce(F.col("as_if_loss_ratio"), F.lit(0.7))) / 0.28, 3)))
            .withColumn("_silver_built_at", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md ## silver_inforce_treaties — typed in-force book with zone names (for portfolio + crux correlation)

# COMMAND ----------

@dlt.table(
    name="silver_inforce_treaties",
    comment="Typed in-force treaty book joined to peak-zone metadata. Source for portfolio accumulation marts "
            "and the crux's correlated-treaty lookup.",
    table_properties={"quality": "silver", "layer": "silver"},
)
def silver_inforce_treaties():
    zones = spark.read.table(f"{REF}.ref_peak_zones").select("zone_id", "zone_name", "peril", "region", "is_peak")
    cedants = spark.read.table(f"{REF}.ref_cedants").select("cedant_id", "cedant_name", "rating")
    return (spark.read.table(f"{REF}.inforce_treaties")
            .join(zones, "zone_id", "left")
            .join(cedants, "cedant_id", "left")
            .withColumn("_silver_built_at", F.current_timestamp()))
