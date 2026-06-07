# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup + synthetic data generation — Bricksurance Re
# MAGIC
# MAGIC Deterministic (seed=42), rolling-date synthetic universe for the Reinsurance Intelligence Workbench.
# MAGIC Generates the **landing** front door (GRLC/CDR-shaped submissions + premium/loss bordereaux + exposure),
# MAGIC the **vendor cat output** (3 divergent vendors), the **in-force portfolio** (as-at snapshot, heavy in
# MAGIC European windstorm), **counterparties**, and reference data. Both hero submissions are hand-seeded and sacred.
# MAGIC
# MAGIC Module boundary: this notebook is standalone, re-runnable and parameterised (catalog/schema/seed widgets).
# MAGIC The reset job calls it with the same seed so dates re-anchor to `current_date()` byte-identically.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
dbutils.widgets.text("seed", "42")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
SEED = int(dbutils.widgets.get("seed"))
fqn = f"{catalog}.{schema}"

import random, datetime
from pyspark.sql import functions as F

random.seed(SEED)
TODAY = datetime.date.today()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {fqn}.landing")
print(f"target = {fqn}  seed={SEED}  as_at={TODAY}")

# COMMAND ----------

# MAGIC %md ## Reference data — peak zones, cedants, counterparties

# COMMAND ----------

# Peak zones. European windstorm is primary (where the bomb lands); CEE flood secondary.
# US Atlantic hurricane is parameterised as a documented swap (present, light).
# appetite_pml_1in200_eur = the CRO's risk appetite for that zone at the 1-in-200 (Solvency II) return period.
PEAK_ZONES = [
    # zone_id, zone_name, peril, region, appetite_pml_1in200_eur, is_peak
    ("EU_WIND",   "European Windstorm",      "Windstorm", "Europe",       500_000_000, True),
    ("CEE_FLOOD", "Central & Eastern Europe Flood", "Flood", "CEE",       200_000_000, True),
    ("US_ATL_HU", "US Atlantic Hurricane",   "Hurricane", "US",           750_000_000, True),   # documented swap
    ("EU_MOTOR",  "European Motor (non-cat)","Motor",     "Europe",                 0, False),  # not a cat-accumulation zone
    ("EU_LIAB",   "European Liability (non-cat)","Liability","Europe",              0, False),
]
zones_df = spark.createDataFrame(
    PEAK_ZONES,
    "zone_id string, zone_name string, peril string, region string, appetite_pml_1in200_eur long, is_peak boolean",
).withColumn("as_at_date", F.lit(TODAY))
zones_df.write.mode("overwrite").saveAsTable(f"{fqn}.ref_peak_zones")

# COMMAND ----------

# Cedants — Bricksurance SE is one named cedant among several synthetic carriers (cross-demo link, loose).
CEDANTS = [
    # cedant_id, cedant_name, domicile, rating, credit_quality_step, is_reputable
    ("CED01", "Bricksurance SE",        "DE", "A",   2, True),
    ("CED02", "Helvetia Mutual",        "CH", "AA-", 1, True),
    ("CED03", "Nordhaven Insurance",    "SE", "A+",  2, True),
    ("CED04", "Iberia Seguros",         "ES", "A-",  2, True),
    ("CED05", "Adriatic Re-cession Co", "HR", "BBB", 3, True),
    ("CED06", "Albion General",         "GB", "A",   2, True),
    ("CED07", "Vistula Insurance",      "PL", "BBB+",3, True),
    ("CED08", "Rhein Provinzial",       "DE", "AA",  1, True),
]
cedants_df = spark.createDataFrame(
    CEDANTS, "cedant_id string, cedant_name string, domicile string, rating string, credit_quality_step int, is_reputable boolean")
cedants_df.write.mode("overwrite").saveAsTable(f"{fqn}.ref_cedants")

# Counterparties — credit quality (light; not the hero path).
RATING_PD = {"AA": 0.02, "AA-": 0.03, "A+": 0.05, "A": 0.06, "A-": 0.08, "BBB+": 0.15, "BBB": 0.24}
cp_rows = [(c[0], c[1], c[3], c[4], RATING_PD[c[3]], "Stable") for c in CEDANTS]
cp_df = spark.createDataFrame(
    cp_rows, "cedant_id string, cedant_name string, rating string, credit_quality_step int, one_year_pd_pct double, outlook string")
cp_df.write.mode("overwrite").saveAsTable(f"{fqn}.counterparties")
print("reference data written")

# COMMAND ----------

# MAGIC %md ## Vendor cat output — 3 divergent vendors (engine fully abstracted; we ingest, never compute)

# COMMAND ----------

# Per zone, 3 vendors give EP-curve PML/AEP/OEP at 1-in-100 / 1-in-200 / 1-in-250.
# Vendors DISAGREE (±15-25%). Databricks blends them; it never runs a cat engine.
# base_pml_1in200 is the "true" central estimate; vendors scatter around it deterministically.
ZONE_BASE_PML_1IN200 = {
    "EU_WIND":   1_180_000_000,   # market gross; the book's *share* accumulates against appetite below
    "CEE_FLOOD":   420_000_000,
    "US_ATL_HU": 1_900_000_000,
}
VENDORS = [("Vendor_A", 0.92), ("Vendor_B", 1.0), ("Vendor_C", 1.18)]   # divergence multipliers
# return-period scaling off the 1-in-200 anchor
RP_SCALE = {100: 0.74, 200: 1.0, 250: 1.12}
cat_rows = []
for zid, base in ZONE_BASE_PML_1IN200.items():
    for vname, vmult in VENDORS:
        for rp, rps in RP_SCALE.items():
            pml = int(base * vmult * rps)
            aep = int(pml * 1.08)   # annual exceedance slightly above occurrence
            oep = int(pml * 0.96)
            cat_rows.append((zid, vname, rp, pml, aep, oep))
cat_df = spark.createDataFrame(
    cat_rows, "zone_id string, vendor string, return_period int, pml_eur long, aep_eur long, oep_eur long"
).withColumn("as_at_date", F.lit(TODAY))
cat_df.write.mode("overwrite").saveAsTable(f"{fqn}.cat_vendor_curves")
print(f"cat_vendor_curves: {cat_df.count()} rows ({len(ZONE_BASE_PML_1IN200)} zones x {len(VENDORS)} vendors x {len(RP_SCALE)} RPs)")

# COMMAND ----------

# MAGIC %md ## In-force portfolio — as-at snapshot, heavily written in European windstorm

# COMMAND ----------

# Background in-force treaties. We deliberately over-index EU_WIND so the book sits *near* appetite
# there (97.6% utilised) — that is what makes sub:900002 the straw that breaches it. Per-treaty PML
# contributions are calibrated to hit deterministic per-zone targets (a diversified zone PML is not the
# naive sum of layer limits; we model the post-diversification contribution and scale to the as-at target).
STRUCTURES = ["Quota Share", "Surplus", "Cat XoL", "Risk XoL"]
# As-at current 1-in-200 PML the book has already consumed per peak zone (drives utilisation vs appetite).
ZONE_CURRENT_PML_TARGET = {"EU_WIND": 488_000_000, "CEE_FLOOD": 96_000_000, "US_ATL_HU": 150_000_000}
inforce = []  # (tid, ced, lob, zid, struct, att, lim, prem, el, raw_contrib, is_corr)

# --- three SACRED in-force treaties that sub:900002 correlates with (named, EU_WIND Property Cat XoL) ---
# raw_contrib ~= limit * occupancy(~0.45); scaled with the zone below.
CORRELATED = [
    ("TR-EUW-101", "CED08", "Property", "EU_WIND", "Cat XoL", 25_000_000, 75_000_000, 8_400_000, 34_000_000),
    ("TR-EUW-102", "CED02", "Property", "EU_WIND", "Cat XoL", 20_000_000, 60_000_000, 6_900_000, 27_000_000),
    ("TR-EUW-103", "CED03", "Property", "EU_WIND", "Cat XoL", 30_000_000, 90_000_000, 9_600_000, 40_000_000),
]
for tid, ced, lob, zid, struct, att, lim, prem, raw_contrib in CORRELATED:
    inforce.append([tid, ced, lob, zid, struct, att, lim, prem, int(prem * 0.42), raw_contrib, True])

# --- background book: scatter, but pile into EU_WIND ---
zone_weights = ["EU_WIND"] * 9 + ["CEE_FLOOD"] * 4 + ["US_ATL_HU"] * 2 + ["EU_MOTOR"] * 3 + ["EU_LIAB"] * 2
for i in range(47):
    tid = f"TR-{1000+i}"
    ced = random.choice([c[0] for c in CEDANTS])
    zid = random.choice(zone_weights)
    lob = {"EU_WIND": "Property", "CEE_FLOOD": "Property", "US_ATL_HU": "Property",
           "EU_MOTOR": "Motor", "EU_LIAB": "Liability"}[zid]
    struct = random.choice(STRUCTURES) if zid in ("EU_MOTOR", "EU_LIAB") else random.choice(["Cat XoL", "Surplus", "Quota Share"])
    att = random.choice([10_000_000, 15_000_000, 20_000_000, 25_000_000])
    lim = random.choice([40_000_000, 50_000_000, 60_000_000, 75_000_000])
    prem = random.randint(3_000_000, 11_000_000)
    el = int(prem * random.uniform(0.35, 0.55))
    raw_contrib = int(lim * random.uniform(0.40, 0.58)) if zid in ZONE_CURRENT_PML_TARGET else 0
    inforce.append([tid, ced, lob, zid, struct, att, lim, prem, el, raw_contrib, False])

# Scale each peak zone's raw contributions so the as-at total lands exactly on target (deterministic).
raw_by_zone = {}
for row in inforce:
    raw_by_zone[row[3]] = raw_by_zone.get(row[3], 0) + row[9]
for row in inforce:
    zid = row[3]
    if zid in ZONE_CURRENT_PML_TARGET and raw_by_zone[zid] > 0:
        row[9] = int(round(row[9] * ZONE_CURRENT_PML_TARGET[zid] / raw_by_zone[zid]))

inforce = [tuple(r) for r in inforce]
inforce_df = spark.createDataFrame(
    inforce,
    "treaty_id string, cedant_id string, lob string, zone_id string, structure string, "
    "attachment_eur long, limit_eur long, ceded_premium_eur long, expected_loss_eur long, "
    "modeled_pml_1in200_contrib_eur long, is_correlated_ref boolean",
).withColumn("inception_date", F.date_sub(F.lit(TODAY), F.lit(random.randint(30, 300))) ) \
 .withColumn("as_at_date", F.lit(TODAY))
inforce_df.write.mode("overwrite").saveAsTable(f"{fqn}.inforce_treaties")

# As-at accumulation by peak zone (the CRO control-tower truth). Current PML = sum of in-force contributions.
acc = (spark.table(f"{fqn}.inforce_treaties")
       .groupBy("zone_id").agg(F.sum("modeled_pml_1in200_contrib_eur").alias("current_pml_1in200_eur"),
                               F.count("*").alias("n_treaties"))
       .join(zones_df.select("zone_id", "zone_name", "peril", "region", "appetite_pml_1in200_eur", "is_peak"), "zone_id")
       .filter("is_peak = true")
       .withColumn("headroom_eur", F.col("appetite_pml_1in200_eur") - F.col("current_pml_1in200_eur"))
       .withColumn("utilisation_pct", F.round(F.col("current_pml_1in200_eur") / F.col("appetite_pml_1in200_eur") * 100, 1))
       .withColumn("as_at_date", F.lit(TODAY)))
acc.write.mode("overwrite").saveAsTable(f"{fqn}.inforce_accumulation")
acc.select("zone_id", "current_pml_1in200_eur", "appetite_pml_1in200_eur", "utilisation_pct", "n_treaties").show(truncate=False)

# COMMAND ----------

# MAGIC %md ## Landing — submissions (MRC-slip / GRLC-CDR shaped) + bordereaux + exposure

# COMMAND ----------

# Channels: ADEPT_CDR = clean structured inbound (ACORD GRLC-shaped); MANUAL = messy email/spreadsheet path.
def make_submission(pid, ced, broker, struct, prop_xol, lob, terr, perils, zid, att, lim,
                    subject_prem, ceded_share, rol, elr, channel, completeness):
    return (pid, ced, broker, struct, prop_xol, lob, terr, perils, zid, int(att), int(lim),
            int(subject_prem), float(ceded_share), float(rol), float(elr), channel, float(completeness))

SUB_SCHEMA = ("submission_public_id string, cedant_id string, broker string, structure string, "
              "proportional_or_xol string, lob string, territories string, perils string, zone_id string, "
              "layer_attachment_eur long, layer_limit_eur long, subject_premium_eur long, "
              "ceded_share_pct double, rol_pct double, expected_loss_ratio double, inbound_channel string, "
              "slip_completeness double")

subs = []

# ============================ SACRED HERO 1: sub:900001 — clean fast-track ============================
# European Motor Quota Share, reputable cedant (Bricksurance SE), clean bordereaux, adequate rate,
# peril/territory away from peak cat, negligible marginal accumulation, good counterparty → recommend-to-bind.
subs.append(make_submission(
    "sub:900001", "CED01", "Guy Carpenter", "Quota Share", "Proportional", "Motor",
    "DE, AT, NL", "Motor TPL, Motor OD", "EU_MOTOR",
    0, 0, subject_prem=240_000_000, ceded_share=0.30, rol=0.0, elr=0.66,
    channel="ADEPT_CDR", completeness=1.0))

# ============================ SACRED HERO 2: sub:900002 — the portfolio bomb ============================
# European Property-Cat XoL €30m xs €20m, reputable cedant, attractive standalone (good RoL, clean-ish
# loss history) — but concentrated in the peak European-windstorm zone the book is already heavy in.
# Breaches appetite, marginal SCR > expected return, correlated with 3 in-force treaties. Toxic in aggregate.
subs.append(make_submission(
    "sub:900002", "CED02", "Aon", "Cat XoL", "XoL", "Property",
    "DE, FR, BE, NL", "Windstorm, Flood", "EU_WIND",
    att=20_000_000, lim=30_000_000, subject_prem=300_000_000, ceded_share=1.0,
    rol=0.182, elr=0.41, channel="ADEPT_CDR", completeness=0.98))

# ============================ background submissions (deterministic) ============================
brokers = ["Aon", "Guy Carpenter", "Gallagher Re", "Lockton Re", "Howden Re"]
for i in range(38):
    pid = f"sub:{900100 + i}"
    ced = random.choice([c[0] for c in CEDANTS])
    zid = random.choice(zone_weights)
    lob = {"EU_WIND": "Property", "CEE_FLOOD": "Property", "US_ATL_HU": "Property",
           "EU_MOTOR": "Motor", "EU_LIAB": "Liability"}[zid]
    is_xol = zid in ZONE_BASE_PML_1IN200 and random.random() < 0.6
    struct = "Cat XoL" if is_xol else random.choice(["Quota Share", "Surplus"])
    channel = "ADEPT_CDR" if random.random() < 0.7 else "MANUAL"
    completeness = round(random.uniform(0.6, 1.0), 2) if channel == "MANUAL" else round(random.uniform(0.94, 1.0), 2)
    if is_xol:
        att = random.choice([10_000_000, 15_000_000, 20_000_000, 25_000_000])
        lim = random.choice([30_000_000, 40_000_000, 50_000_000])
        subs.append(make_submission(pid, ced, random.choice(brokers), struct, "XoL", lob,
                    "DE, FR, NL", "Windstorm" if zid == "EU_WIND" else "Flood", zid, att, lim,
                    random.randint(120_000_000, 350_000_000), 1.0,
                    round(random.uniform(0.09, 0.21), 3), round(random.uniform(0.35, 0.55), 2),
                    channel, completeness))
    else:
        subs.append(make_submission(pid, ced, random.choice(brokers), struct, "Proportional", lob,
                    "DE, AT, NL", "Motor TPL" if lob == "Motor" else "GL", zid, 0, 0,
                    random.randint(80_000_000, 280_000_000), round(random.uniform(0.2, 0.4), 2),
                    0.0, round(random.uniform(0.55, 0.78), 2), channel, completeness))

subs_df = spark.createDataFrame(subs, SUB_SCHEMA) \
    .withColumn("inception_date", F.add_months(F.lit(TODAY), 1)) \
    .withColumn("received_date", F.date_sub(F.lit(TODAY), F.lit(3))) \
    .withColumn("_landing_ingested_at", F.current_timestamp())
subs_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.landing_submissions")
print(f"landing_submissions: {subs_df.count()} (incl. 2 sacred heroes)")

# COMMAND ----------

# Premium + loss bordereaux per submission. Bricksurance SE (CED01) seeded with a book-shaped cession.
sub_list = [(r["submission_public_id"], r["cedant_id"], r["lob"], r["zone_id"], r["expected_loss_ratio"],
             r["subject_premium_eur"]) for r in spark.table(f"{fqn}.landing_submissions").collect()]

prem_rows, loss_rows, exp_rows = [], [], []
for pid, ced, lob, zid, elr, subj in sub_list:
    # premium bordereau: 3 years
    for y in range(3):
        yr = TODAY.year - (2 - y)
        gwp = int(subj * random.uniform(0.9, 1.05))
        prem_rows.append((pid, ced, yr, lob, gwp, random.randint(800, 4200)))
    # loss bordereau: 5 loss years, as-if factored
    for ly in range(5):
        loss_year = TODAY.year - (4 - ly)
        incurred = int(subj * elr * random.uniform(0.5, 1.3))
        large = incurred > subj * elr * 1.1
        loss_rows.append((pid, ced, loss_year, lob,
                          ("Windstorm" if zid == "EU_WIND" else "Flood" if zid == "CEE_FLOOD" else lob),
                          incurred, int(incurred * 0.85), random.randint(20, 600),
                          large, round(random.uniform(1.02, 1.18), 3)))
    # exposure
    exp_rows.append((pid, ced, "Europe", ("Windstorm" if zid == "EU_WIND" else "Flood" if zid == "CEE_FLOOD" else lob),
                     int(subj * random.uniform(3.5, 6.0)), random.randint(1200, 90000), zid))

# --- seed ONE messy loss-bordereau row for the DLT quarantine (bad peril code + null incurred) ---
loss_rows.append(("sub:900133", "CED05", TODAY.year - 1, "Property", "WND-???", None, None, 0, False, 1.0))

spark.createDataFrame(prem_rows,
    "submission_public_id string, cedant_id string, premium_year int, lob string, gwp_eur long, n_risks int") \
    .withColumn("_landing_ingested_at", F.current_timestamp()) \
    .write.mode("overwrite").saveAsTable(f"{fqn}.landing_premium_bordereaux")

spark.createDataFrame(loss_rows,
    "submission_public_id string, cedant_id string, loss_year int, lob string, peril string, "
    "incurred_eur long, paid_eur long, n_claims int, large_loss_flag boolean, as_if_factor double") \
    .withColumn("_landing_ingested_at", F.current_timestamp()) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.landing_loss_bordereaux")

spark.createDataFrame(exp_rows,
    "submission_public_id string, cedant_id string, region string, peril string, tiv_eur long, n_locations int, zone_id string") \
    .withColumn("_landing_ingested_at", F.current_timestamp()) \
    .write.mode("overwrite").saveAsTable(f"{fqn}.landing_exposure")

print("bordereaux + exposure written (incl. 1 seeded messy loss row → quarantine)")

# COMMAND ----------

# MAGIC %md ## Tagging + summary

# COMMAND ----------

for t in ["ref_peak_zones", "ref_cedants", "counterparties", "cat_vendor_curves", "inforce_treaties",
          "inforce_accumulation", "landing_submissions", "landing_premium_bordereaux",
          "landing_loss_bordereaux", "landing_exposure"]:
    try:
        spark.sql(f"ALTER TABLE {fqn}.{t} SET TAGS ('project' = 'reinsurance_workbench', 'demo' = 'bricksurance_re')")
    except Exception as e:
        print(f"tag skip {t}: {str(e)[:80]}")

print("=== generation complete ===")
for t in ["landing_submissions", "landing_premium_bordereaux", "landing_loss_bordereaux",
          "landing_exposure", "cat_vendor_curves", "inforce_treaties", "inforce_accumulation"]:
    print(f"  {t:32s} {spark.table(f'{fqn}.{t}').count():>6} rows")
