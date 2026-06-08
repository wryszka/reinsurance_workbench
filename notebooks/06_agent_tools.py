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
# Aggregate the feature row to exactly one row (deterministic) BEFORE calling ai_query — a SQL scalar UDF body
# that scans a table by key is treated as a correlated scalar subquery and must be provably single-row, and
# ai_query (non-deterministic) cannot itself be wrapped in an aggregate.
agg_cols = ", ".join([f"any_value({c}) AS {c}" for c in FEATURES])

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
    FROM (SELECT {agg_cols} FROM {fqn}.feature_submission WHERE submission_public_id = p_submission_public_id)
  )
""")
print("fn_triage_submission created")

# COMMAND ----------

# MAGIC %md ## fn_price_submission

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_price_submission(p_submission_public_id STRING)
RETURNS STRUCT<predicted_loss_ratio DOUBLE, combined_ratio_pct DOUBLE, offered_rol_pct DOUBLE, rate_adequacy DOUBLE, verdict STRING>
COMMENT 'Technically price a reinsurance submission the REINSURANCE way — by rate-on-line, expected loss, burning cost and exposure/experience rating over the loss bordereaux. This is NOT a primary-insurance frequency-severity GLM and uses none of the pricing_workbench pricing models. Returns the model expected (burning-cost) loss ratio, the projected combined ratio (loss + expense), the offered rate-on-line, a rate-adequacy ratio (target combined / projected combined; >=1 is adequate), and a verdict (adequate / thin / inadequate). Use when asked whether a submission is adequately rated standalone.'
RETURN
  SELECT named_struct(
    'predicted_loss_ratio', round(plr, 4),
    'combined_ratio_pct', round(combined * 100, 1),
    'offered_rol_pct', round(offered_rol * 100, 2),
    'rate_adequacy', round(0.95 / nullif(combined, 0), 3),
    'verdict', CASE WHEN 0.95 / nullif(combined, 0) >= 1.0 THEN 'adequate'
                    WHEN 0.95 / nullif(combined, 0) >= 0.92 THEN 'thin' ELSE 'inadequate' END
  )
  FROM (
    SELECT plr, offered_rol,
           plr + CASE WHEN is_cat_xol = 1 THEN 0.15 ELSE 0.28 END AS combined
    FROM (
      SELECT ai_query('{PRICING_EP}', named_struct({struct_cols}), 'DOUBLE') AS plr,
             rol_pct AS offered_rol, is_cat_xol
      FROM (SELECT {agg_cols} FROM {fqn}.feature_submission WHERE submission_public_id = p_submission_public_id)
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
    'submission_public_id', any_value(submission_public_id), 'cedant', any_value(cedant_name), 'broker', any_value(broker),
    'structure', any_value(structure), 'lob', any_value(lob), 'territories', any_value(territories),
    'perils', any_value(perils), 'zone', any_value(zone_name),
    'layer', any_value(CASE WHEN is_cat_xol = 1 THEN concat(format_number(layer_limit_eur/1e6,0), 'm xs ', format_number(layer_attachment_eur/1e6,0), 'm') ELSE 'proportional' END),
    'subject_premium_eur', any_value(CAST(subject_premium_eur AS DOUBLE)), 'rol_pct', any_value(round(rol_pct*100,2)),
    'rating', any_value(rating), 'data_quality_score', any_value(data_quality_score), 'inbound_channel', any_value(inbound_channel))
  FROM {fqn}.silver_submissions WHERE submission_public_id = p_submission_public_id
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_portfolio_position(p_zone_id STRING)
RETURNS STRUCT<zone_id STRING, zone_name STRING, current_pml_1in200_eur DOUBLE, appetite_eur DOUBLE,
               utilisation_pct DOUBLE, headroom_eur DOUBLE, rag STRING, n_treaties INT>
COMMENT 'Return the current portfolio accumulation position for a peak zone (or pass NULL / "ALL" for the worst-utilised zone): current 1-in-200 PML, appetite, utilisation %, headroom, RAG status and treaty count. Use for CRO control-tower and "Ask the Portfolio" questions about capacity vs appetite.'
RETURN
  SELECT named_struct('zone_id', any_value(zone_id), 'zone_name', any_value(zone_name),
    'current_pml_1in200_eur', any_value(CAST(current_pml_1in200_eur AS DOUBLE)), 'appetite_eur', any_value(CAST(appetite_pml_1in200_eur AS DOUBLE)),
    'utilisation_pct', any_value(utilisation_pct), 'headroom_eur', any_value(CAST(headroom_eur AS DOUBLE)), 'rag', any_value(rag), 'n_treaties', any_value(n_treaties))
  FROM {fqn}.gold_portfolio_position
  WHERE (p_zone_id IS NOT NULL AND p_zone_id <> 'ALL' AND zone_id = p_zone_id)
     OR ((p_zone_id IS NULL OR p_zone_id = 'ALL') AND utilisation_pct = (SELECT max(utilisation_pct) FROM {fqn}.gold_portfolio_position))
""")
print("fn_submission_summary + fn_portfolio_position created")
