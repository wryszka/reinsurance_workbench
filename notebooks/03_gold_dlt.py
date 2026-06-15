# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold (DLT) — CRO control-tower marts
# MAGIC
# MAGIC Read-only marts for the CRO control tower, the app and Genie: vendor-blended cat curves (3 divergent
# MAGIC vendors → one blended view), portfolio position by peak zone (capacity vs appetite, PML 1-in-100/200/250),
# MAGIC and an illustrative capital position (SCR by zone, diversified BSCR, Solvency II 1-in-200 cross-link).
# MAGIC
# MAGIC Module boundary: gold marts are read-only sources. No business logic downstream recomputes them.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("source_catalog")
SCHEMA = spark.conf.get("source_schema")
REF = f"{CATALOG}.{SCHEMA}"

# Illustrative capital parameters (flagged in the app's About box — not a calibrated internal model).
SCR_CAPITAL_FACTOR = 0.55      # SCR held per unit of 1-in-200 zone PML (post net-of-reinstatement)
DIVERSIFICATION_BENEFIT = 0.18  # cross-zone diversification haircut on the sum of standalone SCRs
OWN_FUNDS_EUR = 600_000_000   # eligible own funds (illustrative → ~180% Solvency II ratio)

# COMMAND ----------

# MAGIC %md ## Vendor-blended cat curves — 3 divergent vendors → one blended view + divergence band

# COMMAND ----------

@dlt.table(
    name="gold_cat_blended",
    comment="Per zone/return-period blended cat output across the 3 vendors (mean), with the vendor min/max and "
            "divergence %. Databricks blends vendor EP curves; it never runs a cat engine.",
    table_properties={"quality": "gold", "layer": "gold"},
)
def gold_cat_blended():
    return (spark.read.table(f"{REF}.cat_vendor_curves")
            .groupBy("zone_id", "return_period").agg(
                F.round(F.avg("pml_eur")).cast("long").alias("blended_pml_eur"),
                F.min("pml_eur").alias("vendor_min_pml_eur"),
                F.max("pml_eur").alias("vendor_max_pml_eur"),
                F.count("*").alias("n_vendors"))
            .withColumn("divergence_pct",
                        F.round((F.col("vendor_max_pml_eur") - F.col("vendor_min_pml_eur"))
                                / F.col("blended_pml_eur") * 100, 1))
            .withColumn("_gold_built_at", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md ## Portfolio position by peak zone — capacity vs appetite (CRO control tower)

# COMMAND ----------

@dlt.table(
    name="gold_portfolio_position",
    comment="CRO control-tower mart: per peak zone the current 1-in-200 PML the book has consumed, the appetite, "
            "headroom, utilisation %, treaty count and blended market PML at 1-in-100/200/250. Opening/closing frame.",
    table_properties={"quality": "gold", "layer": "gold"},
)
def gold_portfolio_position():
    acc = spark.read.table(f"{REF}.inforce_accumulation").select(
        "zone_id", "zone_name", "peril", "region", "current_pml_1in200_eur",
        "appetite_pml_1in200_eur", "headroom_eur", "utilisation_pct", "n_treaties")
    blended = dlt.read("gold_cat_blended")
    pml100 = blended.filter("return_period = 100").select("zone_id", F.col("blended_pml_eur").alias("market_pml_1in100_eur"))
    pml200 = blended.filter("return_period = 200").select("zone_id", F.col("blended_pml_eur").alias("market_pml_1in200_eur"))
    pml250 = blended.filter("return_period = 250").select("zone_id", F.col("blended_pml_eur").alias("market_pml_1in250_eur"))
    return (acc
            .join(pml100, "zone_id", "left").join(pml200, "zone_id", "left").join(pml250, "zone_id", "left")
            .withColumn("rag",
                        F.when(F.col("utilisation_pct") >= 95, F.lit("RED"))
                         .when(F.col("utilisation_pct") >= 80, F.lit("AMBER"))
                         .otherwise(F.lit("GREEN")))
            .withColumn("_gold_built_at", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md ## Capital position — SCR by zone + diversified BSCR + Solvency II 1-in-200 cross-link (illustrative)

# COMMAND ----------

@dlt.table(
    name="gold_capital_position",
    comment="Illustrative capital mart: standalone SCR per peak zone (capital held per unit of 1-in-200 PML), the "
            "diversified BSCR after a cross-zone diversification benefit, eligible own funds and the resulting "
            "Solvency II ratio. The 1-in-200 return period is the Solvency II / capital cross-link.",
    table_properties={"quality": "gold", "layer": "gold"},
)
def gold_capital_position():
    from pyspark.sql.window import Window
    pos = dlt.read("gold_portfolio_position").select("zone_id", "zone_name", "current_pml_1in200_eur")
    w = Window.partitionBy()  # whole-frame window — no collect(), DLT-safe
    return (pos
            .withColumn("standalone_scr_eur",
                        (F.col("current_pml_1in200_eur") * F.lit(SCR_CAPITAL_FACTOR)).cast("long"))
            .withColumn("sum_standalone_scr_eur", F.sum("standalone_scr_eur").over(w).cast("long"))
            .withColumn("diversification_benefit_pct", F.lit(round(DIVERSIFICATION_BENEFIT * 100, 1)))
            .withColumn("diversified_bscr_eur",
                        (F.col("sum_standalone_scr_eur") * F.lit(1 - DIVERSIFICATION_BENEFIT)).cast("long"))
            .withColumn("eligible_own_funds_eur", F.lit(OWN_FUNDS_EUR).cast("long"))
            .withColumn("solvency_ratio_pct",
                        F.round(F.lit(OWN_FUNDS_EUR) / F.col("diversified_bscr_eur") * 100, 1))
            .withColumn("_gold_built_at", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md ## Geospatial exposure accumulation — H3 binning + CRESTA rollup

# COMMAND ----------

@dlt.table(
    name="gold_exposure_accumulation",
    comment="Geospatial exposure rollup: per-location TIV binned to H3 cells (h3_longlatash3, GA) and aggregated "
            "by CRESTA zone. Shows real geospatial accumulation on the lakehouse.",
    table_properties={"quality": "gold", "layer": "gold"},
)
def gold_exposure_accumulation():
    loc = spark.read.table(f"{REF}.exposure_locations").withColumn("h3_cell", F.expr("h3_longlatash3(lon, lat, 4)"))
    return (loc.groupBy("zone_id", "cresta").agg(
                F.sum("tiv_eur").cast("long").alias("total_tiv_eur"),
                F.count("*").alias("n_locations"),
                F.countDistinct("h3_cell").alias("h3_cells"),
                F.round(F.avg("lat"), 3).alias("centroid_lat"),
                F.round(F.avg("lon"), 3).alias("centroid_lon"))
            .withColumn("_gold_built_at", F.current_timestamp()))
