# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Reset — re-anchor + cache clear
# MAGIC
# MAGIC One entrypoint. The reset job re-runs the generator (seed=42 → dates re-anchor to `current_date()`),
# MAGIC full-refreshes the medallion pipeline, rebuilds features and re-seeds the governance audit. This notebook
# MAGIC clears the LLM-narration cache (so the demo starts clean) and verifies the heroes still land deterministically.
# MAGIC Models are NOT retrained (retrain=false default). Pre-warming happens on first hero open (or via 98_smoke_test).

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

# Clear the narration cache (structured outputs are never cached).
spark.sql(f"CREATE TABLE IF NOT EXISTS {fqn}.cache_agent_responses (cache_key STRING, endpoint STRING, response STRING, created_ts TIMESTAMP) USING DELTA")
spark.sql(f"TRUNCATE TABLE {fqn}.cache_agent_responses")
print("narration cache cleared")

# COMMAND ----------

# Verify the heroes are deterministic and the story holds after re-anchor.
import datetime
today = datetime.date.today()
maxd = spark.sql(f"SELECT max(received_date) d FROM {fqn}.landing_submissions").collect()[0]["d"]
print("max received_date:", maxd, "| today:", today)

for cid in ["sub:900001", "sub:900002"]:
    acc = spark.sql(f"SELECT {fqn}.fn_accumulation_impact('{cid}') a").collect()[0]["a"]
    cap = spark.sql(f"SELECT {fqn}.fn_capital_impact('{cid}') a").collect()[0]["a"]
    print(f"{cid}: breaches={acc['breaches_appetite']} breach={acc['breach_amount_eur']/1e6:.1f}m "
          f"n_corr={acc['n_correlated']} | RoRAC={cap['rorac_pct']}% destructive={cap['capital_destructive']}")
print("reset verification complete")
