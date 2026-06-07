# Databricks notebook source
# MAGIC %md
# MAGIC # 05b · THE CRUX — marginal accumulation + capital at submission
# MAGIC
# MAGIC The Hitchcock moment's engine and the one genuinely new component. Two **deterministic, explicable UC
# MAGIC functions** (not ML — actuarial math): `fn_accumulation_impact` and `fn_capital_impact`. They read the
# MAGIC silver submission, the as-at zone accumulation and the in-force book, and quantify what binding the deal
# MAGIC does to peak-zone PML and to marginal capital. Everything downstream calls these; nothing recomputes them.
# MAGIC
# MAGIC For the heroes (deterministic):
# MAGIC - `sub:900002`: marginal EU-windstorm PML tips the book over appetite; marginal SCR ≫ expected return
# MAGIC   (RoRAC below hurdle → capital-destructive); correlated with 3 in-force treaties.
# MAGIC - `sub:900001`: ~zero marginal accumulation, RoRAC above hurdle → recommend-to-bind.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

# ── crux constants (illustrative actuarial parameters; flagged in the app's About box) ──
XOL_OCCUPANCY        = 0.92   # share of a peak-zone cat layer expected exhausted at the 1-in-200
XOL_CORR_UPLIFT      = 0.04   # uplift for adding correlated peak-zone exposure
SCR_FACTOR           = 0.55   # capital held per unit of marginal 1-in-200 PML
CORR_LOADING         = 0.25   # extra capital loading per correlated in-force treaty (no diversification)
PROP_SCR_FACTOR      = 0.30   # marginal SCR per unit of proportional ceded premium (premium+reserve risk)
PROP_DIV_CREDIT      = 0.85   # diversification credit for a non-peak proportional line
HURDLE               = 0.15   # RoRAC hurdle: below this the deal is capital-destructive
BROKERAGE            = 0.10   # XoL brokerage
XOL_INTERNAL         = 0.05   # XoL internal expense
CEDING_COMMISSION    = 0.25   # proportional ceding commission
PROP_INTERNAL        = 0.03   # proportional internal expense

# COMMAND ----------

# MAGIC %md ## fn_accumulation_impact — marginal peak-zone PML vs appetite

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_accumulation_impact(p_submission_public_id STRING)
RETURNS STRUCT<
  zone_id STRING, zone_name STRING, structure STRING,
  current_pml_1in200_eur DOUBLE, appetite_eur DOUBLE, headroom_before_eur DOUBLE,
  marginal_pml_1in200_eur DOUBLE, pml_after_eur DOUBLE, headroom_after_eur DOUBLE,
  breaches_appetite BOOLEAN, breach_amount_eur DOUBLE,
  n_correlated INT, correlated_treaty_ids ARRAY<STRING>,
  reasons ARRAY<STRING>>
COMMENT 'Quantify the MARGINAL accumulation impact of binding a reinsurance submission on its peak-zone 1-in-200 PML versus the CRO risk appetite. Returns current vs post-deal PML, headroom, whether appetite is breached and by how much, the count and ids of correlated in-force treaties, and plain-English reasons. Use when an underwriter or the CRO asks what a specific submission does to portfolio accumulation. Deterministic actuarial math, not a model.'
RETURN
  SELECT named_struct(
    'zone_id', s.zone_id, 'zone_name', s.zone_name, 'structure', s.structure,
    'current_pml_1in200_eur', cur, 'appetite_eur', app, 'headroom_before_eur', app - cur,
    'marginal_pml_1in200_eur', marg, 'pml_after_eur', cur + marg, 'headroom_after_eur', app - (cur + marg),
    'breaches_appetite', (cur + marg) > app,
    'breach_amount_eur', greatest(0.0, (cur + marg) - app),
    'n_correlated', ncorr, 'correlated_treaty_ids', array_sort(corr_ids),
    'reasons', array_compact(array(
      CASE WHEN marg > 0 THEN concat('Adds ', format_number(marg/1e6, 1), 'm to ', s.zone_name, ' 1-in-200 PML') END,
      CASE WHEN (cur + marg) > app THEN concat('Breaches ', s.zone_name, ' appetite by ',
            format_number(((cur + marg) - app)/1e6, 1), 'm (', format_number((cur+marg)/app*100, 1), '% of appetite)') END,
      CASE WHEN ncorr > 0 THEN concat('Correlated with ', CAST(ncorr AS STRING), ' in-force ', s.zone_name, ' treaties') END,
      CASE WHEN marg = 0 THEN 'Peril/territory away from a peak cat-accumulation zone — negligible marginal PML' END
    ))
  )
  FROM (
    SELECT any_value(zone_id) AS zone_id, any_value(zone_name) AS zone_name, any_value(structure) AS structure,
           any_value(CAST(coalesce(zone_current_pml_1in200_eur, 0) AS DOUBLE)) AS cur,
           any_value(CAST(coalesce(appetite_pml_1in200_eur, 0) AS DOUBLE)) AS app,
           any_value(CAST(CASE WHEN is_peak AND is_cat_xol = 1
                     THEN layer_limit_eur * {XOL_OCCUPANCY} * (1 + {XOL_CORR_UPLIFT})
                     ELSE 0 END AS DOUBLE)) AS marg,
           any_value(coalesce(n_correlated, 0)) AS ncorr,
           any_value(coalesce(correlated_treaty_ids, array())) AS corr_ids
    FROM {fqn}.silver_submissions
    WHERE submission_public_id = p_submission_public_id
  ) s
""")
print("fn_accumulation_impact created")

# COMMAND ----------

# MAGIC %md ## fn_capital_impact — marginal SCR vs expected return (RoRAC vs hurdle)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn}.fn_capital_impact(p_submission_public_id STRING)
RETURNS STRUCT<
  structure STRING, ceded_premium_eur DOUBLE, expected_loss_eur DOUBLE, expense_eur DOUBLE,
  expected_return_eur DOUBLE, marginal_scr_eur DOUBLE, rorac_pct DOUBLE, hurdle_pct DOUBLE,
  capital_destructive BOOLEAN, n_correlated INT, reasons ARRAY<STRING>>
COMMENT 'Quantify the MARGINAL capital impact of binding a reinsurance submission: ceded premium, expected loss, expenses, expected return, marginal SCR (capital required), and the resulting RoRAC versus the 15% hurdle. Flags capital_destructive when RoRAC is below the hurdle (marginal SCR is large relative to the return the deal earns). Use when an underwriter or the CRO asks whether a submission earns its capital. Deterministic actuarial math, not a model. The 1-in-200 PML is the Solvency II capital cross-link.'
RETURN
  SELECT named_struct(
    'structure', structure, 'ceded_premium_eur', ceded_prem, 'expected_loss_eur', exp_loss,
    'expense_eur', expense, 'expected_return_eur', exp_return, 'marginal_scr_eur', marg_scr,
    'rorac_pct', round(rorac * 100, 1), 'hurdle_pct', {HURDLE} * 100,
    'capital_destructive', rorac < {HURDLE}, 'n_correlated', ncorr,
    'reasons', array_compact(array(
      concat('Expected return ', format_number(exp_return/1e6, 2), 'm on marginal SCR ',
             format_number(marg_scr/1e6, 1), 'm'),
      concat('RoRAC ', format_number(rorac*100, 1), '% vs ', CAST(CAST({HURDLE}*100 AS INT) AS STRING), '% hurdle'),
      CASE WHEN rorac < {HURDLE} THEN 'Capital-destructive: marginal SCR is not earned by the deal' END,
      CASE WHEN ncorr > 0 THEN concat('Capital loaded for correlation with ', CAST(ncorr AS STRING),
             ' in-force treaties (no diversification benefit)') END,
      CASE WHEN rorac >= {HURDLE} THEN 'Clears the capital hurdle — capital-accretive' END
    ))
  )
  FROM (
    WITH base AS (
      SELECT any_value(structure) AS structure, any_value(is_cat_xol) AS icx, any_value(is_peak) AS is_peak,
             any_value(layer_limit_eur) AS layer_limit_eur, any_value(rol_pct) AS rol_pct,
             any_value(ceded_share_pct) AS ceded_share_pct, any_value(subject_premium_eur) AS subject_premium_eur,
             any_value(expected_loss_ratio) AS elr, any_value(coalesce(n_correlated, 0)) AS ncorr
      FROM {fqn}.silver_submissions
      WHERE submission_public_id = p_submission_public_id
    ),
    econ AS (
      SELECT structure, ncorr, icx,
             CASE WHEN icx = 1 THEN rol_pct * layer_limit_eur ELSE ceded_share_pct * subject_premium_eur END AS ceded_prem,
             CAST(CASE WHEN is_peak AND icx = 1 THEN layer_limit_eur * {XOL_OCCUPANCY} * (1 + {XOL_CORR_UPLIFT}) ELSE 0 END AS DOUBLE) AS marg,
             elr
      FROM base
    )
    SELECT structure, ncorr, ceded_prem,
           elr * ceded_prem AS exp_loss,
           CASE WHEN icx = 1 THEN ({BROKERAGE} + {XOL_INTERNAL}) * ceded_prem ELSE ({CEDING_COMMISSION} + {PROP_INTERNAL}) * ceded_prem END AS expense,
           (ceded_prem - elr * ceded_prem - (CASE WHEN icx = 1 THEN ({BROKERAGE} + {XOL_INTERNAL}) * ceded_prem ELSE ({CEDING_COMMISSION} + {PROP_INTERNAL}) * ceded_prem END)) AS exp_return,
           CASE WHEN icx = 1 THEN marg * {SCR_FACTOR} * (1 + {CORR_LOADING} * ncorr) ELSE ceded_prem * {PROP_SCR_FACTOR} * {PROP_DIV_CREDIT} END AS marg_scr,
           CASE WHEN (CASE WHEN icx = 1 THEN marg * {SCR_FACTOR} * (1 + {CORR_LOADING} * ncorr) ELSE ceded_prem * {PROP_SCR_FACTOR} * {PROP_DIV_CREDIT} END) > 0
                THEN (ceded_prem - elr * ceded_prem - (CASE WHEN icx = 1 THEN ({BROKERAGE} + {XOL_INTERNAL}) * ceded_prem ELSE ({CEDING_COMMISSION} + {PROP_INTERNAL}) * ceded_prem END))
                     / (CASE WHEN icx = 1 THEN marg * {SCR_FACTOR} * (1 + {CORR_LOADING} * ncorr) ELSE ceded_prem * {PROP_SCR_FACTOR} * {PROP_DIV_CREDIT} END)
                ELSE 999 END AS rorac
    FROM econ
  ) f
""")
print("fn_capital_impact created")

# COMMAND ----------

# MAGIC %md ## Light check — both heroes

# COMMAND ----------

for cid in ["sub:900001", "sub:900002"]:
    acc = spark.sql(f"SELECT {fqn}.fn_accumulation_impact('{cid}') AS a").collect()[0]["a"]
    cap = spark.sql(f"SELECT {fqn}.fn_capital_impact('{cid}') AS c").collect()[0]["c"]
    print(f"\n=== {cid} ===")
    print(f"  ACCUM: zone={acc['zone_name']} marginal={acc['marginal_pml_1in200_eur']/1e6:.1f}m "
          f"breaches={acc['breaches_appetite']} breach={acc['breach_amount_eur']/1e6:.1f}m n_corr={acc['n_correlated']}")
    print(f"  CAPITAL: exp_return={cap['expected_return_eur']/1e6:.2f}m marg_scr={cap['marginal_scr_eur']/1e6:.1f}m "
          f"RoRAC={cap['rorac_pct']}% destructive={cap['capital_destructive']}")
