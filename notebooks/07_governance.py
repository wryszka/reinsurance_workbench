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

# The "collect once, surface many ways by role" story: every source has a sensitivity tier AND an explicit
# record of WHERE it is surfaced in the workbench and FOR WHOM — one governed copy, many role-specific lenses.
inv = [
    # table, fields, source, tier, pii, retention, used_for, surfaced_as, audience
    ("landing_submissions", "cedant, broker, structure, layers, RoL, perils", "ADEPT/CDR + manual", "Confidential", False, "7y", "Triage + pricing", "Renewal Desk grid · Document-AI slip view · pricing tool", "Underwriters, pricing actuaries"),
    ("landing_mrc_extractions", "Document-AI extracted slip fields + confidence", "Unstructured MRC slips", "Confidential", False, "7y", "Auto-extract → triage + pricing", "Ingestion ▸ Document AI panel", "Underwriters"),
    ("landing_premium_bordereaux", "GWP, risk counts by year", "Cedant bordereaux", "Confidential", False, "7y", "Rate adequacy", "Ingestion feed map · pricing inputs", "Pricing actuaries"),
    ("landing_loss_bordereaux", "incurred, paid, peril, as-if", "Cedant bordereaux", "Confidential", False, "10y", "Loss history / burning cost", "Pricing burning-cost · experience rating", "Pricing actuaries"),
    ("landing_exposure", "TIV, locations, zone", "Cedant exposure", "Confidential", False, "7y", "Accumulation", "Geospatial H3 accumulation · CRO peak-zone PML", "Cat managers, CRO"),
    ("cat_vendor_curves", "EP curves PML/AEP/OEP (3 vendors)", "External cat vendors", "Restricted", False, "perpetual", "Accumulation + capital (blended)", "Control Tower vendor-divergence · accumulation drill", "Cat managers, CRO"),
    ("inforce_treaties", "in-force book, ceded premium, PML contribution", "Internal book of record", "Restricted", False, "perpetual", "Portfolio accumulation", "Control Tower zone drill · marginal accumulation", "CRO, capital team"),
    ("counterparties", "credit rating, PD, credit quality step, watch note", "Rating agencies", "Restricted", False, "perpetual", "Counterparty / credit (PD masked)", "Counterparty panel (PD masked) · sanctions screen", "Credit risk, compliance"),
    ("gold_event_response", "event loss, reinstatements, solvency hit", "Derived from in-force book", "Internal", False, "10y", "Cat-event response", "Cat Event page · Deal track", "CRO, capital team"),
    ("gov_decision_audit", "decision trail, recommendation, bind, who/when", "Internal", "Restricted", False, "10y", "Governance / regulator", "Governance ▸ Decisions · Deal track", "Compliance, regulator, internal audit"),
]
spark.createDataFrame(inv, "table_name string, fields string, source string, sensitivity_tier string, "
                      "contains_pii boolean, retention string, used_for string, surfaced_as string, audience string") \
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

# MAGIC %md ## AI activity / agent-reasoning audit — the governed record of what each agent contributed
# MAGIC The explainability layer for a regulator: which agent looked at each decision, the UC-function tools it
# MAGIC called, and the signal it returned. Derived from the deterministic UC-function outputs + the agent roster
# MAGIC (the live tool-calling agent that produces these is on the Reinsurance AI page).

# COMMAND ----------

act = [
    # subject_id, subject_kind, agent_name, agent_role, tools_used, signal, reasoning_text
    ("sub:900002", "submission", "Reinsurance AI supervisor", "supervisor", "fn_triage_submission, fn_price_submission, fn_accumulation_impact, fn_capital_impact, fn_recommendation", "REFER", "Attractive standalone (56% combined) but binding it adds 28.7m of windstorm PML into a peak zone already at 97.6% of appetite — breaches appetite by 16.7m and RoRAC of 8.7% sits below the 15% hurdle. Refer, do not bind."),
    ("sub:900002", "submission", "Challenge / Second-Opinion", "challenge", "fn_accumulation_impact, fn_capital_impact", "REFER", "Stress-tested the other side: even resized smaller the marginal capital stays destructive and the deal is correlated with three in-force EU-windstorm treaties. The diversification argument does not hold here."),
    ("sub:900002", "submission", "Portfolio Strategy", "portfolio", "fn_portfolio_alternative", "ALTERNATIVE", "Capacity is better spent on US Atlantic hurricane (~19.5% RoRAC) where the book has headroom — same premium, accretive instead of destructive."),
    ("sub:900002", "submission", "Counterparty Credit", "counterparty", "counterparties, gov_counterparty_secure", "CLEAR", "Helvetia Mutual AA-, credit quality step 1, no sanctions hit and no regulatory-watch flag. Counterparty is not the issue — the accumulation is."),
    ("sub:900002", "submission", "Data Quality", "dataquality", "gold_dq_scorecard", "OK", "Slip extracted at high confidence; exposure and bordereaux complete, nothing quarantined. The numbers can be trusted."),
    ("sub:900001", "submission", "Reinsurance AI supervisor", "supervisor", "fn_triage_submission, fn_price_submission, fn_accumulation_impact, fn_capital_impact, fn_recommendation", "RECOMMEND-TO-BIND", "Clean motor quota share, in appetite, adequate price (94% combined), ~zero marginal peak-zone accumulation and RoRAC ~23% — accretive. Recommend to bind; quote and move on."),
    ("sub:900001", "submission", "Challenge / Second-Opinion", "challenge", "fn_accumulation_impact, fn_capital_impact", "CONCUR", "No peak-cat accumulation and the deal diversifies the book. No objection to binding."),
    ("sub:900001", "submission", "Portfolio Strategy", "portfolio", "fn_portfolio_alternative", "CONCUR", "Diversifies away from the windstorm peak — a good use of capacity."),
    ("sub:900001", "submission", "Counterparty Credit", "counterparty", "counterparties, gov_counterparty_secure", "CLEAR", "Bricksurance SE, investment grade, clean on sanctions and watch lists."),
    ("sub:900001", "submission", "Data Quality", "dataquality", "gold_dq_scorecard", "OK", "ADEPT/CDR clean feed, complete submission — no data-quality concerns."),
    ("evt:900001", "event", "Cat-Event Response", "event", "fn_event_response, fn_event_treaty_detail", "BRIEF", "Windstorm Eckhart, NW Europe: 22 treaties respond, gross 150m, net 133m after 17m reinstatement income. Most exposed cedant Helvetia at 50m. Solvency II 181% to 141% — above the 100% floor."),
]
from pyspark.sql import functions as _F
spark.createDataFrame([(f"AIA-{i+1:03d}", *r) for i, r in enumerate(act)],
    "activity_id string, subject_id string, subject_kind string, agent_name string, agent_role string, "
    "tools_used string, signal string, reasoning_text string") \
    .withColumn("created_ts", _F.current_timestamp()) \
    .write.mode("overwrite").saveAsTable(f"{fqn}.gov_ai_activity")
print("gov_ai_activity seeded:", spark.table(f"{fqn}.gov_ai_activity").count(), "rows")

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

# MAGIC %md ## Real UC dynamic data masking — a governed view with a column mask

# COMMAND ----------

# Real Unity Catalog column-masking: sensitive counterparty fields are visible only to a privileged group;
# everyone else sees a redaction. Demonstrated on a governed VIEW (the base table stays readable for the
# decision engine + agents). This is standard UC governance — is_account_group_member drives the mask.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.mask_sensitive(v STRING)
RETURNS STRING
COMMENT 'UC column-mask helper: returns the value to members of bricksurance_re_secret_readers, else a redaction.'
RETURN CASE WHEN is_account_group_member('bricksurance_re_secret_readers') THEN v ELSE '*** restricted ***' END
""")
spark.sql(f"""
CREATE OR REPLACE VIEW {fqn}.gov_counterparty_secure AS
SELECT cedant_id, cedant_name, rating, credit_quality_step, regulatory_watch,
       {fqn}.mask_sensitive(CAST(round(one_year_pd_pct,3) AS STRING)) AS one_year_pd_pct,
       {fqn}.mask_sensitive(watch_note) AS watch_note
FROM {fqn}.counterparties
""")
print("UC masking function + governed view created")

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
