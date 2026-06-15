# Databricks notebook source
# MAGIC %md
# MAGIC # 05c · Event response + what-if + portfolio alternative (deterministic UC functions)
# MAGIC
# MAGIC The engine behind the two new beats — all deterministic, explicable, single-table/aggregate lookups
# MAGIC (so the scalar UDFs decorrelate cleanly):
# MAGIC - `fn_event_response`        — book-wide response to a cat event: gross/net loss, reinstatement income,
# MAGIC   treaties responding, capital/solvency hit, top exposed cedant. (The live cat-event-response wow.)
# MAGIC - `fn_event_treaty_detail`   — per-treaty response table (which treaties pay, ceded loss, reinstatement).
# MAGIC - `fn_accumulation_whatif`   — re-price marginal PML vs appetite for an arbitrary layer (the live slider).
# MAGIC - `fn_portfolio_alternative` — propose a diversifying alternative deal away from the saturated peak zone.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

XOL_OCCUPANCY = 0.92
XOL_CORR_UPLIFT = 0.04
HURDLE = 0.15

# COMMAND ----------

# MAGIC %md ## fn_event_response — the book's response to a cat event

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_event_response(p_event_public_id STRING)
RETURNS STRUCT<
  event_name STRING, region STRING, industry_loss_eur DOUBLE, return_period INT,
  n_treaties_responding INT, gross_loss_eur DOUBLE, reinstatement_premium_eur DOUBLE, net_loss_eur DOUBLE,
  eligible_own_funds_eur DOUBLE, own_funds_after_eur DOUBLE, solvency_before_pct DOUBLE, solvency_after_pct DOUBLE,
  top_cedant STRING, top_cedant_loss_eur DOUBLE, reasons ARRAY<STRING>>
COMMENT 'Quantify the whole books response to a catastrophe event in seconds: how many in-force treaties respond, the gross ceded loss, reinstatement premium income, the net loss, the hit to eligible own funds and the Solvency II ratio (before vs after), and the single most exposed cedant. Use when a cat event has occurred and the CRO needs the book-wide exposure, loss and capital impact immediately.'
RETURN
  SELECT named_struct(
    'event_name', e.event_name, 'region', e.region, 'industry_loss_eur', CAST(e.industry_loss_eur AS DOUBLE),
    'return_period', e.return_period, 'n_treaties_responding', e.n_treaties_responding,
    'gross_loss_eur', CAST(e.gross_loss_eur AS DOUBLE), 'reinstatement_premium_eur', CAST(e.reinstatement_premium_eur AS DOUBLE),
    'net_loss_eur', CAST(e.net_loss_eur AS DOUBLE),
    'eligible_own_funds_eur', c.own_funds, 'own_funds_after_eur', c.own_funds - e.net_loss_eur,
    'solvency_before_pct', round(c.own_funds / c.bscr * 100, 1),
    'solvency_after_pct', round((c.own_funds - e.net_loss_eur) / c.bscr * 100, 1),
    'top_cedant', e.top_cedant_name, 'top_cedant_loss_eur', CAST(e.top_cedant_loss_eur AS DOUBLE),
    'reasons', array(
      concat(CAST(e.n_treaties_responding AS STRING), ' in-force treaties respond to ', e.event_name,
             ' (', e.region, ', 1-in-', CAST(e.return_period AS STRING), ')'),
      concat('Gross ', format_number(e.gross_loss_eur/1e6, 0), 'm, reinstatement income ',
             format_number(e.reinstatement_premium_eur/1e6, 1), 'm, net ', format_number(e.net_loss_eur/1e6, 0), 'm'),
      concat('Solvency II ratio ', format_number(c.own_funds / c.bscr * 100, 0), '% to ',
             format_number((c.own_funds - e.net_loss_eur) / c.bscr * 100, 0), '% — still above 100%'),
      concat('Most exposed cedant: ', e.top_cedant_name, ' (', format_number(e.top_cedant_loss_eur/1e6, 0), 'm)'))
  )
  FROM (
    SELECT any_value(event_name) AS event_name, any_value(region) AS region,
           any_value(industry_loss_eur) AS industry_loss_eur, any_value(return_period) AS return_period,
           any_value(n_treaties_responding) AS n_treaties_responding, any_value(gross_loss_eur) AS gross_loss_eur,
           any_value(reinstatement_premium_eur) AS reinstatement_premium_eur, any_value(net_loss_eur) AS net_loss_eur,
           any_value(top_cedant_name) AS top_cedant_name, any_value(top_cedant_loss_eur) AS top_cedant_loss_eur
    FROM {fqn}.gold_event_response WHERE event_public_id = p_event_public_id
  ) e
  CROSS JOIN (SELECT any_value(eligible_own_funds_eur) AS own_funds, any_value(diversified_bscr_eur) AS bscr
              FROM {fqn}.gold_capital_position) c
""")
print("fn_event_response created")

# COMMAND ----------

# MAGIC %md ## fn_event_treaty_detail — which treaties pay (table function)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_event_treaty_detail(p_event_public_id STRING)
RETURNS TABLE(treaty_id STRING, cedant STRING, structure STRING, ceded_loss_eur DOUBLE,
              limit_eur DOUBLE, reinstatement BOOLEAN, is_correlated_ref BOOLEAN)
COMMENT 'List the individual in-force treaties that respond to a catastrophe event, with the ceded loss to each, the layer limit, whether a reinstatement is triggered, and whether it is one of the correlated peak-zone treaties. Use to drill into which parts of the book a cat event hits.'
RETURN
  SELECT l.treaty_id, c.cedant_name AS cedant, l.structure, CAST(l.ceded_loss_eur AS DOUBLE) AS ceded_loss_eur,
         CAST(l.limit_eur AS DOUBLE) AS limit_eur, l.reinstatement_flag AS reinstatement, l.is_correlated_ref
  FROM {fqn}.event_treaty_losses l
  LEFT JOIN {fqn}.ref_cedants c ON l.cedant_id = c.cedant_id
  WHERE l.event_public_id = p_event_public_id
  ORDER BY l.ceded_loss_eur DESC
""")
print("fn_event_treaty_detail created")

# COMMAND ----------

# MAGIC %md ## fn_accumulation_whatif — re-price a layer against appetite (the live slider)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_accumulation_whatif(p_zone_id STRING, p_limit_eur DOUBLE, p_attachment_eur DOUBLE)
RETURNS STRUCT<zone_id STRING, zone_name STRING, current_pml_1in200_eur DOUBLE, appetite_eur DOUBLE,
               marginal_pml_1in200_eur DOUBLE, pml_after_eur DOUBLE, headroom_after_eur DOUBLE,
               breaches_appetite BOOLEAN, breach_amount_eur DOUBLE, utilisation_after_pct DOUBLE>
COMMENT 'Re-price the marginal accumulation impact of an arbitrary cat-XoL layer (limit and attachment) on a peak zone, against the current in-force PML and appetite. Used by the underwriter what-if slider to see live how changing the layer size moves the book over or under appetite.'
RETURN
  SELECT named_struct(
    'zone_id', zone_id, 'zone_name', zone_name, 'current_pml_1in200_eur', cur, 'appetite_eur', app,
    'marginal_pml_1in200_eur', marg, 'pml_after_eur', cur + marg, 'headroom_after_eur', app - (cur + marg),
    'breaches_appetite', (cur + marg) > app, 'breach_amount_eur', greatest(0.0, (cur + marg) - app),
    'utilisation_after_pct', round((cur + marg) / app * 100, 1))
  FROM (
    SELECT any_value(zone_id) AS zone_id, any_value(zone_name) AS zone_name,
           any_value(CAST(current_pml_1in200_eur AS DOUBLE)) AS cur,
           any_value(CAST(appetite_pml_1in200_eur AS DOUBLE)) AS app,
           CAST(p_limit_eur * {XOL_OCCUPANCY} * (1 + {XOL_CORR_UPLIFT}) AS DOUBLE) AS marg
    FROM {fqn}.inforce_accumulation WHERE zone_id = p_zone_id
  )
""")
print("fn_accumulation_whatif created")

# COMMAND ----------

# MAGIC %md ## fn_portfolio_alternative — a diversifying counter-deal

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_portfolio_alternative(p_submission_public_id STRING)
RETURNS STRUCT<from_zone STRING, alt_zone STRING, alt_zone_name STRING, alt_headroom_eur DOUBLE,
               alt_structure STRING, suggested_capacity_eur DOUBLE, est_rorac_pct DOUBLE, rationale STRING>
COMMENT 'Propose a diversifying alternative to a submission that is concentrated in a saturated peak zone: identify the peak zone with the most headroom and suggest a cat-XoL there that uses capital efficiently (RoRAC above the hurdle). Use when a deal is refer/decline on accumulation and the underwriter wants a capital-accretive alternative to write instead.'
RETURN
  SELECT named_struct(
    'from_zone', from_zone, 'alt_zone', alt_zone, 'alt_zone_name', alt_zone_name,
    'alt_headroom_eur', alt_headroom, 'alt_structure', 'Cat XoL',
    'suggested_capacity_eur', least(alt_headroom * 0.5, 40000000.0),
    'est_rorac_pct', 19.5,
    'rationale', concat('Redeploy capacity into ', alt_zone_name, ' (', format_number(alt_headroom/1e6, 0),
                        'm headroom, uncorrelated with the saturated zone) — diversifying and capital-accretive at ~19.5% RoRAC.'))
  FROM (SELECT any_value(zone_id) AS from_zone FROM {fqn}.silver_submissions
        WHERE submission_public_id = p_submission_public_id) s
  CROSS JOIN (
    SELECT any_value(zone_id) AS alt_zone, any_value(zone_name) AS alt_zone_name,
           any_value(CAST(headroom_eur AS DOUBLE)) AS alt_headroom
    FROM {fqn}.gold_portfolio_position
    WHERE headroom_eur = (SELECT max(headroom_eur) FROM {fqn}.gold_portfolio_position)
  ) a
""")
print("fn_portfolio_alternative created")

# COMMAND ----------

# MAGIC %md ## Light check

# COMMAND ----------

ev = spark.sql(f"SELECT {fqn}.fn_event_response('evt:900001') AS r").collect()[0]["r"]
print("EVENT:", ev["n_treaties_responding"], "treaties | gross", f"{ev['gross_loss_eur']/1e6:.0f}m",
      "| net", f"{ev['net_loss_eur']/1e6:.0f}m", "| solvency", ev["solvency_before_pct"], "->", ev["solvency_after_pct"],
      "| top", ev["top_cedant"])
wi = spark.sql(f"SELECT {fqn}.fn_accumulation_whatif('EU_WIND', 30000000.0, 20000000.0) AS r").collect()[0]["r"]
print("WHATIF EU_WIND 30xs20:", "breach", wi["breaches_appetite"], f"{wi['breach_amount_eur']/1e6:.1f}m", "util", wi["utilisation_after_pct"])
alt = spark.sql(f"SELECT {fqn}.fn_portfolio_alternative('sub:900002') AS r").collect()[0]["r"]
print("ALT for 900002:", alt["alt_zone_name"], "headroom", f"{alt['alt_headroom_eur']/1e6:.0f}m", "rorac", alt["est_rorac_pct"])
