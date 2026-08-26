# Databricks notebook source
# MAGIC %md
# MAGIC # 90 · Ingestion examples — the reinsurance feeds customers ask about
# MAGIC
# MAGIC A **standalone catalogue** of example ingestion methods for the data a reinsurer actually receives. Each
# MAGIC example lands an `ex_*` table with a few quality rules and registers itself in `ingestion_methods_catalog`.
# MAGIC **Nothing here is wired into pricing / accumulation / capital** — these are reusable templates to connect later.
# MAGIC
# MAGIC Methods shown: **Auto Loader** (cloudFiles, schema evolution), **read_files** (CSV/JSON/Parquet), **Document AI**
# MAGIC (see 01b), reference/API pulls, and semi-structured message parsing. Patterns that need an external system
# MAGIC (Lakehouse Federation / Lakeflow Connect / Kafka) are registered as `pattern` and documented in
# MAGIC `docs/INGESTION_METHODS.md`.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"
import os, json
from pyspark.sql import functions as F
VOL = f"/Volumes/{catalog}/{schema}/landing"
EX = f"{VOL}/examples"
for d in ["bdx_autoloader", "cat_elt", "exposure_sov", "claims_movements", "ebot", "_schema", "_chk"]:
    os.makedirs(f"{EX}/{d}", exist_ok=True)

CATALOG_ROWS = []
def register(source, category, fmt, method, feature, table, rows, rules, status="example", notes=""):
    CATALOG_ROWS.append((source, category, fmt, method, feature, f"{fqn}.{table}" if table else None,
                         int(rows), rules, status, notes))
    print(f"[{status}] {source:34s} via {method:22s} -> {table or '(pattern)'}  ({rows} rows)")

# COMMAND ----------

# MAGIC %md ## 1 · Bordereaux (premium & loss) — Auto Loader (cloudFiles) + schema evolution
# MAGIC The canonical reinsurance feed: monthly cedant spreadsheets, every cedant a slightly different layout.

# COMMAND ----------

open(f"{EX}/bdx_autoloader/cedant_alpha_2026m01.csv", "w").write(
    "policy_ref,uw_year,lob,gwp_eur,commission_eur,n_risks\n"
    "PA-1001,2026,Property,1250000,187500,320\n"
    "PA-1002,2026,Property,640000,96000,150\n")
open(f"{EX}/bdx_autoloader/cedant_beta_2026m01.csv", "w").write(   # drifted layout from a different cedant system
    "policy_ref,underwriting_year,line_of_business,gross_premium,brokerage,risk_count,scheme_code\n"
    "PB-2001,2026,Motor,980000,98000,5400,SCH-7\n")
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv").option("header", "true")
    .option("cloudFiles.schemaLocation", f"{EX}/_schema/bdx")
    .option("cloudFiles.schemaEvolutionMode", "rescue").option("rescuedDataColumn", "_rescued_data")
    .load(f"{EX}/bdx_autoloader/")
    .withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_file", F.col("_metadata.file_path"))
    .writeStream.option("checkpointLocation", f"{EX}/_chk/bdx").trigger(availableNow=True)
    .toTable(f"{fqn}.ex_bordereaux"))
# (availableNow blocks until the micro-batch finishes)
import time; time.sleep(2)
n = spark.table(f"{fqn}.ex_bordereaux").count()
register("Premium / loss bordereaux", "Cedant feeds", "CSV / Excel (per-cedant layouts)", "Auto Loader (cloudFiles)",
         "Lakeflow / Structured Streaming", "ex_bordereaux", n,
         "schema evolution (rescue) · _rescued_data captures drift · positive GWP",
         notes="Incremental file drop to a Volume; handles each cedant's differing schema without breaking.")

# COMMAND ----------

# MAGIC %md ## 2 · Cat-model loss output — vendor ELT / YLT (read_files)
# MAGIC Event Loss Table / Year Loss Table from RMS / Verisk. We ingest and blend; we never run the cat engine.

# COMMAND ----------

elt = spark.createDataFrame(
    [("EU_WIND", "Vendor_A", 100023, 0.004, 41_000_000), ("EU_WIND", "Vendor_A", 100024, 0.0021, 88_000_000),
     ("EU_WIND", "Vendor_B", 200011, 0.0035, 52_000_000), ("CEE_FLOOD", "Vendor_A", 300002, 0.006, 22_000_000)],
    "zone_id string, vendor string, event_id long, rate double, gross_loss_eur long")
elt.write.format("parquet").mode("overwrite").save(f"{EX}/cat_elt/")
ex_elt = (spark.read.parquet(f"{EX}/cat_elt/").withColumn("_ingested_at", F.current_timestamp()))
ex_elt.write.mode("overwrite").saveAsTable(f"{fqn}.ex_cat_elt")
register("Cat-model ELT / YLT", "Cat models", "Parquet / CSV (vendor export)", "read_files (Volume)",
         "Unity Catalog Volumes", "ex_cat_elt", ex_elt.count(),
         "non-negative loss · rate in (0,1] · known zone",
         notes="Per-event/year loss tables from RMS/Verisk; aggregated to PML/AEP downstream when wired.")

# COMMAND ----------

# MAGIC %md ## 3 · Exposure — SOV / OED (location-level TIV), read_files + geospatial-ready

# COMMAND ----------

open(f"{EX}/exposure_sov/sov_cedant_alpha.csv", "w").write(
    "loc_id,cedant,country,cresta,lat,lon,tiv_eur,occupancy,construction\n"
    "L-001,Bricksurance SE,DE,DE-1,51.2,6.8,42000000,industrial,reinforced concrete\n"
    "L-002,Bricksurance SE,NL,NL-1,52.1,4.9,18500000,commercial,masonry\n"
    "L-003,Helvetia Mutual,FR,FR-2,48.9,2.3,77000000,commercial,steel\n")
ex_sov = (spark.read.option("header", "true").option("inferSchema", "true").csv(f"{EX}/exposure_sov/")
          .withColumn("_ingested_at", F.current_timestamp()))
ex_sov.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.ex_exposure_sov")
register("Exposure — SOV / OED", "Exposure", "CSV (ACORD/OED Statement of Values)", "read_files (Volume)",
         "Unity Catalog Volumes + H3 geospatial", "ex_exposure_sov", ex_sov.count(),
         "positive TIV · valid lat/long · CRESTA present",
         notes="Location-level TIV; bin with h3_longlatash3 for geospatial accumulation (see gold_exposure_accumulation).")

# COMMAND ----------

# MAGIC %md ## 4 · Large-loss / claims movements — incremental feed (Auto Loader)

# COMMAND ----------

open(f"{EX}/claims_movements/move_2026m01.json", "w").write("\n".join(json.dumps(r) for r in [
    {"claim_ref": "CLM-7001", "treaty_id": "TR-EUW-101", "movement": "paid", "amount_eur": 1200000, "as_of": "2026-01-15"},
    {"claim_ref": "CLM-7001", "treaty_id": "TR-EUW-101", "movement": "reserve", "amount_eur": 800000, "as_of": "2026-01-15"},
    {"claim_ref": "CLM-7002", "treaty_id": "TR-EUW-103", "movement": "paid", "amount_eur": 5400000, "as_of": "2026-01-20"}]))
(spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{EX}/_schema/clm")
    .load(f"{EX}/claims_movements/").withColumn("_ingested_at", F.current_timestamp())
    .writeStream.option("checkpointLocation", f"{EX}/_chk/clm").trigger(availableNow=True).toTable(f"{fqn}.ex_claims_movements"))
import time; time.sleep(2)
register("Large-loss / claims movements", "Claims", "JSON / CSV (paid & reserve movements)", "Auto Loader (cloudFiles)",
         "Lakeflow / Structured Streaming", "ex_claims_movements", spark.table(f"{fqn}.ex_claims_movements").count(),
         "non-negative amount · known movement type",
         notes="Incremental paid/reserve movements per claim & treaty; feeds reserving / IBNR if wired later.")

# COMMAND ----------

# MAGIC %md ## 5 · FX / currency rates — reference feed (API pull simulated)

# COMMAND ----------

fx = spark.createDataFrame(
    [("EUR", "USD", 1.08, "2026-01-31"), ("EUR", "GBP", 0.84, "2026-01-31"), ("EUR", "CHF", 0.95, "2026-01-31"),
     ("EUR", "PLN", 4.32, "2026-01-31"), ("EUR", "JPY", 168.5, "2026-01-31")],
    "base_ccy string, quote_ccy string, rate double, rate_date string").withColumn("_ingested_at", F.current_timestamp())
fx.write.mode("overwrite").saveAsTable(f"{fqn}.ex_fx_rates")
register("FX / currency rates", "Reference", "JSON (REST API)", "REST API pull → Delta",
         "Workflows + requests / Lakeflow Connect", "ex_fx_rates", fx.count(),
         "rate > 0 · base+quote present · one row per pair/day",
         notes="Daily ECB/market rates; reinsurance is multi-currency so every premium/loss is FX-converted.")

# COMMAND ----------

# MAGIC %md ## 6 · Sanctions / watchlist — list ingest + counterparty screening

# COMMAND ----------

wl = spark.createDataFrame(
    [("OFAC SDN", "Sanctioned Holdings Ltd", "RU"), ("EU consolidated", "Adriatic Shell Co", "HR"),
     ("UK HMT", "Vistula Holdings BVI", "VG")],
    "list_name string, entity_name string, domicile string").withColumn("_ingested_at", F.current_timestamp())
wl.write.mode("overwrite").saveAsTable(f"{fqn}.ex_sanctions_list")
# screen cedants by fuzzy-ish name/domicile overlap (illustrative)
screen = (spark.table(f"{fqn}.ref_cedants").alias("c")
          .join(wl.alias("w"), F.col("c.domicile") == F.col("w.domicile"), "left")
          .select("c.cedant_id", "c.cedant_name", "c.domicile",
                  F.when(F.col("w.entity_name").isNotNull(), F.lit("review")).otherwise(F.lit("clear")).alias("screen_result"),
                  F.col("w.list_name").alias("matched_list")))
screen.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.ex_sanctions_screen")
register("Sanctions / watchlists", "Compliance", "CSV / API (OFAC, EU, UK HMT)", "read_files / API pull",
         "Unity Catalog + UC functions", "ex_sanctions_screen", screen.count(),
         "every cedant screened · domicile match flagged for review",
         notes="OFAC/EU/UK consolidated lists; screen cedants & brokers at onboarding and renewal.")

# COMMAND ----------

# MAGIC %md ## 7 · ACORD EBOT / ECOT technical accounts — semi-structured message parse

# COMMAND ----------

open(f"{EX}/ebot/ebot_msg_001.json", "w").write(json.dumps({
    "message_type": "EBOT", "umr": "B1234EXAMPLE", "broker": "Aon", "cedant": "Helvetia Mutual",
    "technical_account": {"settlement_ccy": "EUR", "premium_eur": 5460000, "brokerage_eur": 546000,
                          "taxes_eur": 0, "period": "2026-01"}}))
ex_ebot = (spark.read.option("multiLine", "true").json(f"{EX}/ebot/")
           .select("message_type", "umr", "broker", "cedant",
                   F.col("technical_account.settlement_ccy").alias("settlement_ccy"),
                   F.col("technical_account.premium_eur").alias("premium_eur"),
                   F.col("technical_account.brokerage_eur").alias("brokerage_eur"),
                   F.col("technical_account.period").alias("period"))
           .withColumn("_ingested_at", F.current_timestamp()))
ex_ebot.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.ex_ebot_messages")
register("ACORD EBOT / ECOT", "London market messaging", "JSON / XML (ACORD GRLC technical accounts)",
         "read_files (semi-structured) + parse", "Unity Catalog + variant/JSON", "ex_ebot_messages", ex_ebot.count(),
         "valid message type · UMR present · premium reconciles to slip",
         notes="Electronic Back-Office / Claims technical accounts from PPL / DXC; premiums, brokerage, settlement.")

# COMMAND ----------

# MAGIC %md ## 8 · Counterparty credit ratings — rating-agency feed

# COMMAND ----------

cr = spark.createDataFrame(
    [("Bricksurance SE", "S&P", "A", "Stable", "2026-01-10"), ("Helvetia Mutual", "AM Best", "A+", "Stable", "2026-01-10"),
     ("Vistula Insurance", "S&P", "BBB+", "Negative", "2026-01-10"), ("Adriatic Re-cession Co", "AM Best", "B++", "Stable", "2026-01-10")],
    "cedant_name string, agency string, rating string, outlook string, rating_date string").withColumn("_ingested_at", F.current_timestamp())
cr.write.mode("overwrite").saveAsTable(f"{fqn}.ex_credit_ratings")
register("Counterparty credit ratings", "Reference", "CSV / API (S&P, AM Best, Moody's)", "API pull / read_files",
         "Unity Catalog + Lakeflow Connect", "ex_credit_ratings", cr.count(),
         "rating in allowed scale · agency + date present",
         notes="Feeds counterparty credit-quality step and the capital counterparty-default module.")

# COMMAND ----------

# MAGIC %md ## Patterns that need an external system (registered as templates; code in docs/INGESTION_METHODS.md)

# COMMAND ----------

register("Policy / claims system of record", "Internal systems", "JDBC (SQL Server / Oracle)",
         "Lakehouse Federation", "Lakehouse Federation", None, 0,
         "—", status="pattern", notes="Federated catalog over the admin DB; query in place or CTAS into bronze.")
register("Guidewire / SaaS source", "Internal systems", "Managed connector",
         "Lakeflow Connect", "Lakeflow Connect", None, 0,
         "—", status="pattern", notes="Managed ingestion connector (Salesforce, Workday, SQL Server, ServiceNow, …).")
register("Real-time market / cat feed", "Streaming", "Kafka / Kinesis / EventHubs",
         "Structured Streaming", "Structured Streaming", None, 0,
         "—", status="pattern", notes="True streaming source (live event footprints, market data). Auto Loader covers file-streaming.")

# COMMAND ----------

# MAGIC %md ## Register the catalogue

# COMMAND ----------

cat_df = spark.createDataFrame(CATALOG_ROWS,
    "source_name string, category string, format string, ingestion_method string, databricks_feature string, "
    "landing_table string, example_rows int, quality_rules string, status string, notes string")
cat_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.ingestion_methods_catalog")
print(f"\n=== ingestion_methods_catalog: {cat_df.count()} methods "
      f"({cat_df.filter('status=\"example\"').count()} runnable examples, "
      f"{cat_df.filter('status=\"pattern\"').count()} patterns) ===")
cat_df.select("source_name", "ingestion_method", "status", "example_rows").show(20, truncate=False)
