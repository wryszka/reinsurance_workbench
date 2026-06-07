# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Governance
# MAGIC
# MAGIC Reads existing objects and adds governed tables/functions — no business logic. Submission-to-bind audit
# MAGIC trail, "what's collected" data inventory with sensitivity tiers, sanctions/ESG counterparty checks, and the
# MAGIC capital / Solvency II 1-in-200 cross-link. Tags tables with sensitivity tiers.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## Submission-to-bind audit trail (+ logging function). Heroes seeded deterministically.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fqn}.gov_decision_audit (
  decision_id STRING, submission_public_id STRING, triage_decision STRING, technical_verdict STRING,
  breaches_appetite BOOLEAN, breach_amount_eur DOUBLE, capital_destructive BOOLEAN, rorac_pct DOUBLE,
  recommendation STRING, decided_by STRING, decision_ts TIMESTAMP, bound BOOLEAN
) USING DELTA
""")

# Seed the two heroes' audit rows deterministically from the live UC functions (so the trail reconciles).
spark.sql(f"DELETE FROM {fqn}.gov_decision_audit WHERE submission_public_id IN ('sub:900001','sub:900002')")
for cid, rec, bound in [("sub:900001", "recommend-to-bind", True), ("sub:900002", "refer", False)]:
    tri = spark.sql(f"SELECT {fqn}.fn_triage_submission('{cid}') a").collect()[0]["a"]
    pri = spark.sql(f"SELECT {fqn}.fn_price_submission('{cid}') a").collect()[0]["a"]
    acc = spark.sql(f"SELECT {fqn}.fn_accumulation_impact('{cid}') a").collect()[0]["a"]
    cap = spark.sql(f"SELECT {fqn}.fn_capital_impact('{cid}') a").collect()[0]["a"]
    spark.sql(f"""INSERT INTO {fqn}.gov_decision_audit VALUES (
      'DEC-{cid[-6:]}', '{cid}', '{tri['decision']}', '{pri['verdict']}',
      {str(acc['breaches_appetite']).lower()}, {acc['breach_amount_eur']}, {str(cap['capital_destructive']).lower()},
      {cap['rorac_pct']}, '{rec}', 'demo-underwriter', current_timestamp(), {str(bound).lower()})""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_decision_audit(p_submission_public_id STRING)
RETURNS TABLE(decision_id STRING, recommendation STRING, triage_decision STRING, technical_verdict STRING,
              breaches_appetite BOOLEAN, capital_destructive BOOLEAN, decided_by STRING, decision_ts TIMESTAMP, bound BOOLEAN)
COMMENT 'Return the submission-to-bind decision audit trail for a submission: what was recommended, the triage and pricing verdicts, whether appetite was breached or the deal was capital-destructive, who decided and whether it was bound. Use for governance and regulator questions about how a decision was reached.'
RETURN SELECT decision_id, recommendation, triage_decision, technical_verdict, breaches_appetite,
              capital_destructive, decided_by, decision_ts, bound
       FROM {fqn}.gov_decision_audit WHERE submission_public_id = p_submission_public_id
""")
print("decision audit seeded + fn_decision_audit created")

# COMMAND ----------

# MAGIC %md ## Data inventory — what's collected, sensitivity tier, retention, masking

# COMMAND ----------

inv = [
    # table, fields, source, tier, pii, retention, used_for
    ("landing_submissions", "cedant, broker, structure, layers, RoL, perils", "ADEPT/CDR + manual", "Confidential", False, "7y", "Triage + pricing"),
    ("landing_premium_bordereaux", "GWP, risk counts by year", "Cedant bordereaux", "Confidential", False, "7y", "Rate adequacy"),
    ("landing_loss_bordereaux", "incurred, paid, peril, as-if", "Cedant bordereaux", "Confidential", False, "10y", "Loss history / burning cost"),
    ("landing_exposure", "TIV, locations, zone", "Cedant exposure", "Confidential", False, "7y", "Accumulation"),
    ("cat_vendor_curves", "EP curves PML/AEP/OEP (3 vendors)", "External cat vendors", "Restricted", False, "perpetual", "Accumulation + capital (blended; engine abstracted)"),
    ("inforce_treaties", "in-force book, ceded premium, PML contrib", "Internal", "Restricted", False, "perpetual", "Portfolio accumulation"),
    ("counterparties", "credit rating, PD, credit quality step", "Rating agencies", "Restricted", False, "perpetual", "Counterparty / credit"),
    ("gov_decision_audit", "decision trail, recommendation, bind", "Internal", "Restricted", False, "10y", "Governance / regulator"),
]
spark.createDataFrame(inv, "table_name string, fields string, source string, sensitivity_tier string, "
                      "contains_pii boolean, retention string, used_for string") \
    .write.mode("overwrite").saveAsTable(f"{fqn}.gov_data_inventory")

# Apply sensitivity tier tags (best-effort — governed tag policies may block).
TIER = {r[0]: r[3] for r in inv}
for t, tier in TIER.items():
    try:
        spark.sql(f"ALTER TABLE {fqn}.{t} SET TAGS ('sensitivity' = '{tier}')")
    except Exception as e:
        print(f"tag skip {t}: {str(e)[:60]}")
print("data inventory + sensitivity tags written")

# COMMAND ----------

# MAGIC %md ## Sanctions / ESG counterparty checks (light)

# COMMAND ----------

cedants = spark.table(f"{fqn}.ref_cedants").collect()
checks = []
for c in cedants:
    sanctions = "clear"   # all synthetic cedants screen clear
    esg = "amber" if c["cedant_id"] in ("CED05",) else "green"  # one CEE carrier flagged ESG-amber
    checks.append((c["cedant_id"], c["cedant_name"], c["domicile"], sanctions, esg,
                   "OFAC/EU/UK consolidated lists", "MSCI-style ESG proxy"))
spark.createDataFrame(checks, "cedant_id string, cedant_name string, domicile string, sanctions_status string, "
                      "esg_status string, sanctions_source string, esg_source string") \
    .write.mode("overwrite").saveAsTable(f"{fqn}.gov_counterparty_checks")
print("sanctions/ESG checks written")

# COMMAND ----------

# MAGIC %md ## Capital / Solvency II 1-in-200 cross-link (view over the gold capital mart)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fqn}.gov_solvency_crosslink AS
SELECT zone_id, zone_name, current_pml_1in200_eur AS pml_1in200_eur, standalone_scr_eur,
       diversified_bscr_eur, eligible_own_funds_eur, solvency_ratio_pct,
       'The 1-in-200 (99.5th percentile, 1-year) PML is the Solvency II capital cross-link' AS note
FROM {fqn}.gold_capital_position
""")
print("gov_solvency_crosslink view created")
spark.table(f"{fqn}.gov_decision_audit").select(
    "submission_public_id", "recommendation", "breaches_appetite", "capital_destructive", "rorac_pct", "bound").show(truncate=False)
