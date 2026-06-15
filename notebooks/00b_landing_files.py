# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · Landing files — the hard data types
# MAGIC
# MAGIC Writes the genuinely-difficult inbound formats into the landing Volume so the ingestion layer can show
# MAGIC Databricks doing things that break rigid pipelines:
# MAGIC - **MRC submission slips** (unstructured markdown/ACORD-shaped) → Document AI extraction (01b).
# MAGIC - **Schema-drifted loss bordereaux** (renamed/extra columns) → Auto Loader / read_files rescued data.
# MAGIC - **Cat-event footprint** (semi-structured JSON feed) → read_files JSON.
# MAGIC - **Geospatial exposure** (per-location lat/long + TIV) → H3 binning + CRESTA accumulation.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"
import os, json, random
from pyspark.sql import functions as F
random.seed(42)
BASE = f"/Volumes/{catalog}/{schema}/landing"
for d in ["mrc_slips", "bordereaux_files", "event_feed"]:
    os.makedirs(f"{BASE}/{d}", exist_ok=True)

# COMMAND ----------

# MAGIC %md ## MRC submission slips (unstructured) — incl. the sub:900002 hero + one messy slip

# COMMAND ----------

# Read landing (depends only on 00_setup, not the pipeline) + cedant names + exposure TIV.
_ced = {r["cedant_id"]: r["cedant_name"] for r in spark.table(f"{fqn}.ref_cedants").collect()}
_tiv = {r["submission_public_id"]: r["t"] for r in
        spark.table(f"{fqn}.landing_exposure").groupBy("submission_public_id").agg(F.sum("tiv_eur").alias("t")).collect()}
subs = {}
for r in spark.table(f"{fqn}.landing_submissions").collect():
    d = r.asDict()
    d["cedant_name"] = _ced.get(d["cedant_id"], d["cedant_id"])
    d["is_cat_xol"] = 1 if d["proportional_or_xol"] == "XoL" else 0
    d["total_tiv_eur"] = _tiv.get(d["submission_public_id"], 0)
    subs[d["submission_public_id"]] = d

def slip(s, messy=False):
    lim = f"EUR {int(s['layer_limit_eur']):,}" if s["layer_limit_eur"] else "n/a (proportional)"
    att = f"EUR {int(s['layer_attachment_eur']):,}" if s["layer_attachment_eur"] else "n/a"
    rol = "" if messy else f"\n- **Rate on Line:** {round(float(s['rol_pct'])*100,2)}%"
    struct = ("a layered excess-of-loss arrangement" if messy and s["is_cat_xol"] == 1 else s["structure"])
    return f"""MARKET REFORM CONTRACT — RISK DETAILS
Unique Market Reference (UMR): B{abs(hash(s['submission_public_id']))%10**10:010d}
Submission: {s['submission_public_id']}

REINSURED / CEDANT
- Name: {s['cedant_name']}
- Broker: {s['broker']}

CONTRACT
- Type of reinsurance: {struct} ({s['proportional_or_xol']})
- Class of business: {s['lob']}
- Territory: {s['territories']}
- Perils covered: {s['perils']}
- Period: 12 months at {s['inception_date']}

LIMITS & RETENTION
- Layer limit: {lim}
- Excess of (attachment): {att}
- Cedant subject premium: EUR {int(s['subject_premium_eur']):,}{rol}

GENERAL
This slip is submitted for quotation. Terms as agreed. {'Rate to be confirmed.' if messy else ''}
"""

# A representative set: both heroes + a few background, one deliberately messy (no RoL, fuzzy structure).
ids = ["sub:900001", "sub:900002", "sub:900110", "sub:900115", "sub:900120", "sub:900133"]
written = 0
for i, sid in enumerate(ids):
    if sid not in subs:
        continue
    messy = (sid == "sub:900133")   # the messy slip → low extraction confidence → quarantine
    open(f"{BASE}/mrc_slips/{sid.replace(':','_')}.md", "w").write(slip(subs[sid], messy))
    written += 1
print(f"wrote {written} MRC slips to {BASE}/mrc_slips/")

# COMMAND ----------

# MAGIC %md ## Schema-drifted loss bordereaux (CSV) — a clean file and a drifted one

# COMMAND ----------

# Clean schema: submission_public_id, loss_year, peril, incurred_eur
open(f"{BASE}/bordereaux_files/clean_cedant_A.csv", "w").write(
    "submission_public_id,loss_year,peril,incurred_eur\n"
    "sub:900110,2024,Windstorm,4200000\n"
    "sub:900110,2023,Windstorm,1800000\n"
    "sub:900115,2024,Flood,2600000\n")
# Drifted schema from a different cedant system: renamed cols + an extra column → read_files rescues them.
open(f"{BASE}/bordereaux_files/drift_cedant_B.csv", "w").write(
    "submission_ref,year,peril_code,gross_loss_eur,adjuster_note\n"
    "sub:900120,2024,WND,5100000,large industrial site\n"
    "sub:900120,2022,WND,900000,attritional\n"
    "sub:900124,2023,FLD,3300000,river flood\n")
print("wrote clean + schema-drifted bordereaux CSVs")

# COMMAND ----------

# MAGIC %md ## Cat-event footprint (semi-structured JSON feed)

# COMMAND ----------

ev = spark.table(f"{fqn}.events").collect()[0].asDict()
footprint = {
    "event_public_id": ev["event_public_id"], "event_name": ev["event_name"], "peril": ev["peril"],
    "issued_ts": str(ev["event_date"]), "vendor": "Vendor_B footprint feed", "return_period": int(ev["return_period"]),
    "industry_loss_eur": int(ev["industry_loss_eur"]),
    "affected_cresta": [{"cresta": "DE-1", "severity": 0.9}, {"cresta": "NL-1", "severity": 0.8},
                        {"cresta": "BE-1", "severity": 0.7}, {"cresta": "FR-2", "severity": 0.5}]}
open(f"{BASE}/event_feed/{ev['event_public_id'].replace(':','_')}.json", "w").write(json.dumps(footprint, indent=2))
print("wrote event footprint JSON for", ev["event_public_id"])

# COMMAND ----------

# MAGIC %md ## Geospatial exposure — per-location lat/long + TIV (H3 binning happens in gold)

# COMMAND ----------

# Rough lat/long anchors per peak zone (Europe / US) — a few locations per peak-zone submission.
ANCHOR = {"EU_WIND": (51.0, 7.0), "CEE_FLOOD": (50.0, 19.0), "US_ATL_HU": (28.0, -81.0)}
rows = []
for sid, s in subs.items():
    z = s["zone_id"]
    if z not in ANCHOR or not s.get("total_tiv_eur"):
        continue
    lat0, lon0 = ANCHOR[z]; n = random.randint(3, 6); tiv_each = int(s["total_tiv_eur"]) // n
    for k in range(n):
        rows.append((sid, s["cedant_name"], z,
                     round(lat0 + random.uniform(-2, 2), 4), round(lon0 + random.uniform(-3, 3), 4),
                     tiv_each, f"{z[:2]}-{random.randint(1,3)}"))
spark.createDataFrame(rows, "submission_public_id string, cedant string, zone_id string, lat double, lon double, "
                      "tiv_eur long, cresta string").write.mode("overwrite").saveAsTable(f"{fqn}.exposure_locations")
print(f"exposure_locations: {spark.table(f'{fqn}.exposure_locations').count()} locations")
print("=== landing files complete ===")
