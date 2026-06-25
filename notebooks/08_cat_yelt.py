# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Cat-model output at scale — a YELT landed in Delta
# MAGIC
# MAGIC Cat models (Verisk Touchstone, Moody's RMS) emit **YELTs / PLTs** — Year / Period Event Loss
# MAGIC Tables: huge datasets of simulated event losses. The reinsurance value of Databricks here is
# MAGIC simple — **ingest the massive simulated table into Delta Lake, then query it against the live
# MAGIC in-force book in seconds** to derive the loss distribution (the EP curve, the 1-in-200 PML).
# MAGIC
# MAGIC This notebook builds a synthetic, vendor-style YELT (`cat_yelt`): a 100,000-year stochastic
# MAGIC simulation across the two European cat peak zones, ~380k simulated event losses. The app's
# MAGIC `/api/yelt/ep` endpoint applies the in-force Cat XoL layers (`inforce_treaties`) to every row
# MAGIC and reads the OEP / AEP off the distribution — that is the "query it against the portfolio" beat.
# MAGIC
# MAGIC Production YELTs run to hundreds of millions of rows (per-location / per-contract granularity);
# MAGIC the Delta table and the query are identical — it just scales out.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

N_TRIALS = 100_000  # simulated years

# COMMAND ----------

# MAGIC %md ## Build the YELT
# MAGIC One row = one simulated event in one trial year, with its modelled gross loss to the book.
# MAGIC Per zone: a Poisson-like event frequency (binomial approximation over 6 candidate slots) and a
# MAGIC lognormal severity. Severity is calibrated so the post-layer EP curve grades cleanly through the
# MAGIC 1-in-100 / 200 / 250 and only exhausts the program in the deep tail.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {fqn}.cat_yelt
COMMENT 'Simulated Year Event Loss Table (YELT) — a stochastic-event-set catastrophe-model output landed in Delta. One row = one simulated event in one trial year, with its modelled gross loss to the in-force book. 100,000-year simulation across the European cat book (EU windstorm + CEE flood). Synthetic vendor-style output (Touchstone/RMS pattern); query against the live portfolio to derive the EP curve.'
AS
WITH zones AS (
  SELECT * FROM VALUES
    ('EU_WIND','Windstorm','Europe',     2.5, 28000000, 0.95),
    ('CEE_FLOOD','Flood','CEE',          1.3, 18000000, 0.80)
  AS t(zone_id, peril, region, lambda, sev_med, sigma)
),
trials AS (SELECT explode(sequence(1, {N_TRIALS})) AS trial_id),
slots AS (
  SELECT trial_id, zone_id, peril, region, sev_med, sigma, lambda, s
  FROM trials CROSS JOIN zones LATERAL VIEW explode(sequence(1,6)) AS s
),
occur AS (SELECT * FROM slots WHERE rand() < lambda/6.0)
SELECT trial_id, zone_id, peril, region,
   concat(zone_id, '-E', lpad(cast(cast(floor(rand()*5000) as int) as string), 5, '0')) AS event_id,
   cast(1+floor(rand()*365) as int) AS event_day,
   cast(round(sev_med * exp(sigma*randn())) as bigint) AS gross_loss_eur
FROM occur
""")

display(spark.sql(f"SELECT count(*) rows, count(distinct trial_id) trials, count(distinct event_id) events FROM {fqn}.cat_yelt"))

# COMMAND ----------

# MAGIC %md ## The portfolio query — derive the EP curve by applying the in-force layers
# MAGIC This is exactly what the app runs (`/api/yelt/ep`). The heavy table is generic simulated losses;
# MAGIC the live join applies the book's reinsurance structure to get the reinsurer's loss distribution.

# COMMAND ----------

display(spark.sql(f"""
WITH zone_layer AS (
  SELECT zone_id, SUM(limit_eur) AS zone_limit,
         CAST(ROUND(SUM(attachment_eur*limit_eur)/SUM(limit_eur)) AS BIGINT) AS zone_attach
  FROM {fqn}.inforce_treaties WHERE structure = 'Cat XoL' GROUP BY zone_id),
ev AS (
  SELECT y.trial_id,
         LEAST(GREATEST(y.gross_loss_eur - z.zone_attach, 0), z.zone_limit) AS ceded_eur
  FROM {fqn}.cat_yelt y JOIN zone_layer z USING (zone_id)),
by_year AS (SELECT trial_id, SUM(ceded_eur) AS aep, MAX(ceded_eur) AS oep FROM ev GROUP BY trial_id),
all_trials AS (SELECT explode(sequence(1, {N_TRIALS})) AS trial_id),
dens AS (SELECT a.trial_id, COALESCE(b.aep,0) aep, COALESCE(b.oep,0) oep
         FROM all_trials a LEFT JOIN by_year b USING (trial_id))
SELECT CAST(percentile(oep,0.99)  AS BIGINT) oep_1in100,
       CAST(percentile(oep,0.995) AS BIGINT) oep_1in200,
       CAST(percentile(oep,0.996) AS BIGINT) oep_1in250,
       CAST(percentile(aep,0.995) AS BIGINT) aep_1in200,
       CAST(avg(aep) AS BIGINT) aal,
       CAST(max(oep) AS BIGINT) worst_year
FROM dens
"""))
