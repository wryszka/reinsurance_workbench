# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Agent tools — UC functions
# MAGIC
# MAGIC The atomic, individually-callable UC functions the app panels call live and the supervisor routes off
# MAGIC (rich COMMENTs). Model-backed scorers resolve their serving endpoint by **substring** (dev-prefix safe)
# MAGIC and pass a pre-fetched feature struct to `ai_query` (feature-vector contract, no online store).
# MAGIC
# MAGIC - `fn_triage_submission` — appetite decision + confidence + reasons (model_triage_classifier).
# MAGIC - `fn_price_submission`  — technical price / expected loss ratio / rate adequacy (model_loss_ratio).
# MAGIC - `fn_submission_summary` — deterministic one-call summary of a submission (for narration).
# MAGIC - `fn_portfolio_position` — peak-zone capacity vs appetite (for "Ask the Portfolio").
# MAGIC
# MAGIC (`fn_accumulation_impact` + `fn_capital_impact` — the crux — are created in 05b.)

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
eps = [e.name for e in w.serving_endpoints.list()]
TRIAGE_EP = next((n for n in eps if "reinsurance-triage" in n), "reinsurance-triage")
PRICING_EP = next((n for n in eps if "reinsurance-pricing" in n), "reinsurance-pricing")
print("triage ep:", TRIAGE_EP, "| pricing ep:", PRICING_EP)

FEATURES = ["subject_premium_eur", "ceded_share_pct", "rol_pct", "as_if_loss_ratio", "large_loss_count",
            "total_tiv_eur", "data_quality_score", "rate_adequacy", "is_cat_xol", "credit_quality_step",
            "counterparty_pd_pct", "is_peak_zone", "zone_utilisation_pct", "expected_loss_ratio"]
struct_cols = ", ".join([f"'{c}', {c}" for c in FEATURES])

# COMMAND ----------

# MAGIC %md ## fn_triage_submission

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_triage_submission(p_submission_public_id STRING)
RETURNS STRUCT<decision STRING, confidence DOUBLE, top_reasons ARRAY<STRING>>
COMMENT 'Decide the appetite disposition for a reinsurance submission: fast_track, refer (needs underwriter/accumulation review), or decline. Scores the triage model for a given submission_public_id and returns the decision, a confidence percentage, and plain-English reasons. Use when asked how a specific submission should be triaged.'
RETURN
  SELECT named_struct(
    'decision', element_at(array('fast_track','refer','decline'), CAST(array_position(p, array_max(p)) AS INT)),
    'confidence', round(array_max(p) * 100, 1),
    'top_reasons', array_compact(array(
      CASE WHEN data_quality_score < 0.75 THEN 'Incomplete / manual-path data' END,
      CASE WHEN is_peak_zone = 1 AND is_cat_xol = 1 THEN 'Cat XoL in a peak accumulation zone — needs accumulation review' END,
      CASE WHEN zone_utilisation_pct >= 90 THEN concat('Peak zone already ', format_number(zone_utilisation_pct,1), '% of appetite') END,
      CASE WHEN credit_quality_step >= 4 THEN 'Weaker counterparty credit quality' END,
      CASE WHEN rate_adequacy >= 1.0 AND (is_peak_zone = 0 OR is_cat_xol = 0) THEN 'Adequate rate, away from peak cat' END
    ))
  )
  FROM (
    SELECT ai_query('{TRIAGE_EP}', named_struct({struct_cols}), 'ARRAY<DOUBLE>') AS p,
           data_quality_score, is_peak_zone, is_cat_xol, zone_utilisation_pct, credit_quality_step, rate_adequacy
    FROM {fqn}.feature_submission WHERE submission_public_id = p_submission_public_id
  )
""")
print("fn_triage_submission created")

# COMMAND ----------

# MAGIC %md ## fn_price_submission

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_price_submission(p_submission_public_id STRING)
RETURNS STRUCT<predicted_loss_ratio DOUBLE, technical_rol_pct DOUBLE, offered_rol_pct DOUBLE, rate_adequacy DOUBLE, verdict STRING>
COMMENT 'Technically price a reinsurance submission: returns the model expected (burning-cost) loss ratio, the indicated technical rate-on-line, the offered rate-on-line, the rate adequacy ratio, and a verdict (adequate / thin / inadequate). Use when asked whether a submission is adequately rated or what the technical price is.'
RETURN
  SELECT named_struct(
    'predicted_loss_ratio', round(plr, 4),
    'technical_rol_pct', round(tech_rol * 100, 2),
    'offered_rol_pct', round(offered_rol * 100, 2),
    'rate_adequacy', round(adequacy, 3),
    'verdict', CASE WHEN adequacy >= 1.05 THEN 'adequate' WHEN adequacy >= 0.95 THEN 'thin' ELSE 'inadequate' END
  )
  FROM (
    SELECT plr, offered_rol,
           CASE WHEN is_cat_xol = 1 THEN plr * 0.45 + 0.07 ELSE plr END AS tech_rol,
           CASE WHEN is_cat_xol = 1
                THEN offered_rol / nullif(plr * 0.45 + 0.07, 0)
                ELSE (1.0 - plr) / 0.28 END AS adequacy
    FROM (
      SELECT ai_query('{PRICING_EP}', named_struct({struct_cols}), 'DOUBLE') AS plr,
             rol_pct AS offered_rol, is_cat_xol
      FROM {fqn}.feature_submission WHERE submission_public_id = p_submission_public_id
    )
  )
""")
print("fn_price_submission created")

# COMMAND ----------

# MAGIC %md ## fn_submission_summary + fn_portfolio_position (deterministic, table-read)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_submission_summary(p_submission_public_id STRING)
RETURNS STRUCT<submission_public_id STRING, cedant STRING, broker STRING, structure STRING, lob STRING,
               territories STRING, perils STRING, zone STRING, layer STRING, subject_premium_eur DOUBLE,
               rol_pct DOUBLE, rating STRING, data_quality_score DOUBLE, inbound_channel STRING>
COMMENT 'Return a compact, factual summary of a reinsurance submission (cedant, broker, structure, line of business, territories, perils, peak zone, layer, subject premium, rate-on-line, cedant rating, data quality, inbound channel). Use to brief a user on a specific submission before deeper analysis.'
RETURN
  SELECT named_struct(
    'submission_public_id', submission_public_id, 'cedant', cedant_name, 'broker', broker,
    'structure', structure, 'lob', lob, 'territories', territories, 'perils', perils, 'zone', zone_name,
    'layer', CASE WHEN is_cat_xol = 1 THEN concat(format_number(layer_limit_eur/1e6,0), 'm xs ', format_number(layer_attachment_eur/1e6,0), 'm') ELSE 'proportional' END,
    'subject_premium_eur', CAST(subject_premium_eur AS DOUBLE), 'rol_pct', round(rol_pct*100,2),
    'rating', rating, 'data_quality_score', data_quality_score, 'inbound_channel', inbound_channel)
  FROM {fqn}.silver_submissions WHERE submission_public_id = p_submission_public_id
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_portfolio_position(p_zone_id STRING)
RETURNS STRUCT<zone_id STRING, zone_name STRING, current_pml_1in200_eur DOUBLE, appetite_eur DOUBLE,
               utilisation_pct DOUBLE, headroom_eur DOUBLE, rag STRING, n_treaties INT>
COMMENT 'Return the current portfolio accumulation position for a peak zone (or pass NULL / "ALL" for the worst-utilised zone): current 1-in-200 PML, appetite, utilisation %, headroom, RAG status and treaty count. Use for CRO control-tower and "Ask the Portfolio" questions about capacity vs appetite.'
RETURN
  SELECT named_struct('zone_id', zone_id, 'zone_name', zone_name,
    'current_pml_1in200_eur', CAST(current_pml_1in200_eur AS DOUBLE), 'appetite_eur', CAST(appetite_pml_1in200_eur AS DOUBLE),
    'utilisation_pct', utilisation_pct, 'headroom_eur', CAST(headroom_eur AS DOUBLE), 'rag', rag, 'n_treaties', n_treaties)
  FROM {fqn}.gold_portfolio_position
  WHERE (p_zone_id IS NOT NULL AND p_zone_id <> 'ALL' AND zone_id = p_zone_id)
     OR ((p_zone_id IS NULL OR p_zone_id = 'ALL') AND utilisation_pct = (SELECT max(utilisation_pct) FROM {fqn}.gold_portfolio_position))
  LIMIT 1
""")
print("fn_submission_summary + fn_portfolio_position created")
