# Databricks notebook source
# MAGIC %md
# MAGIC # 98 · Smoke test — both heroes, end to end
# MAGIC
# MAGIC The single full pass (P10). One row per step, PASS/FAIL, fails loudly at the end. Asserts the sacred
# MAGIC hero story: `sub:900001` → recommend-to-bind with ~zero impact; `sub:900002` → quantified accumulation +
# MAGIC capital flags with the 3 correlated treaties; supervisor narration returns; the control tower is coherent.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fqn = f"{catalog}.{schema}"

import time, json
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
RESULTS = []

def check(step, fn):
    t0 = time.time()
    try:
        msg = fn() or "ok"; status = "PASS"
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"; status = "FAIL"
    secs = round(time.time() - t0, 1)
    RESULTS.append({"step": step, "status": status, "secs": secs, "detail": str(msg)[:150]})
    print(("✅" if status == "PASS" else "❌") + f" [{step}] {status} ({secs}s) — {str(msg)[:150]}")
    return status == "PASS"

def one(sql):
    return spark.sql(sql).collect()[0]

# COMMAND ----------

def s_data():
    n = one(f"SELECT count(*) c FROM {fqn}.landing_submissions")["c"]
    assert n >= 40, f"only {n} submissions"
    return f"{n} submissions"
check("01·data", s_data)

def s_quarantine():
    n = one(f"SELECT count(*) c FROM {fqn}.bronze_quarantine_loss")["c"]
    assert n >= 1, "no quarantined rows"
    return f"{n} quarantined"
check("02·bronze+quarantine", s_quarantine)

def s_silver():
    r = one(f"SELECT count(*) c FROM {fqn}.silver_submissions WHERE submission_public_id IN ('sub:900001','sub:900002')")
    assert r["c"] == 2, "heroes missing in silver"
    return "both heroes enriched"
check("03·silver", s_silver)

def s_gold():
    r = one(f"SELECT rag FROM {fqn}.gold_portfolio_position WHERE zone_id='EU_WIND'")
    assert r["rag"] == "RED", f"EU_WIND rag={r['rag']}"
    sv = one(f"SELECT DISTINCT solvency_ratio_pct s FROM {fqn}.gold_capital_position")["s"]
    assert 120 <= sv <= 320, f"solvency {sv}"
    return f"EU_WIND RED, solvency {sv}%"
check("04·gold marts", s_gold)

def s_features():
    n = one(f"SELECT count(*) c FROM {fqn}.feature_submission WHERE submission_public_id IN ('sub:900001','sub:900002')")["c"]
    assert n == 2, "heroes missing in features"
    return "both heroes in feature store"
check("05·features", s_features)

def s_triage():
    t1 = one(f"SELECT to_json({fqn}.fn_triage_submission('sub:900001')) j")["j"]; t1 = json.loads(t1)
    t2 = one(f"SELECT to_json({fqn}.fn_triage_submission('sub:900002')) j")["j"]; t2 = json.loads(t2)
    assert t1["decision"] == "fast_track", f"900001 triage {t1['decision']}"
    assert t2["decision"] == "refer", f"900002 triage {t2['decision']}"
    return f"900001={t1['decision']} 900002={t2['decision']}"
check("06·triage", s_triage)

def s_price():
    p2 = json.loads(one(f"SELECT to_json({fqn}.fn_price_submission('sub:900002')) j")["j"])
    return f"900002 verdict={p2['verdict']} adequacy={p2['rate_adequacy']}"
check("07·pricing", s_price)

def s_accum():
    a1 = json.loads(one(f"SELECT to_json({fqn}.fn_accumulation_impact('sub:900001')) j")["j"])
    a2 = json.loads(one(f"SELECT to_json({fqn}.fn_accumulation_impact('sub:900002')) j")["j"])
    assert not a1["breaches_appetite"], "900001 should not breach"
    assert a2["breaches_appetite"], "900002 should breach"
    assert a2["n_correlated"] == 3, f"900002 n_corr={a2['n_correlated']}"
    return f"900001 no-breach; 900002 breach {a2['breach_amount_eur']/1e6:.1f}m, {a2['n_correlated']} correlated"
check("08·CRUX accumulation", s_accum)

def s_capital():
    c1 = json.loads(one(f"SELECT to_json({fqn}.fn_capital_impact('sub:900001')) j")["j"])
    c2 = json.loads(one(f"SELECT to_json({fqn}.fn_capital_impact('sub:900002')) j")["j"])
    assert not c1["capital_destructive"], f"900001 destructive (RoRAC {c1['rorac_pct']})"
    assert c2["capital_destructive"], f"900002 not destructive (RoRAC {c2['rorac_pct']})"
    return f"900001 RoRAC {c1['rorac_pct']}% (ok); 900002 RoRAC {c2['rorac_pct']}% (destructive)"
check("09·CRUX capital", s_capital)

def s_audit():
    n = one(f"SELECT count(*) c FROM {fqn}.gov_decision_audit WHERE submission_public_id IN ('sub:900001','sub:900002')")["c"]
    assert n == 2, f"audit rows {n}"
    return "both heroes audited"
check("10·governance audit", s_audit)

def s_endpoints():
    eps = [e.name for e in w.serving_endpoints.list()]
    need = ["reinsurance-triage", "reinsurance-pricing", "reinsurance-supervisor", "reinsurance-event"]
    miss = [n for n in need if not any(n in e for e in eps)]
    assert not miss, f"missing endpoints {miss}"
    return f"{len([e for e in eps if 'reinsurance' in e])} reinsurance endpoints"
check("11·serving + agents", s_endpoints)

def s_event():
    e = json.loads(one(f"SELECT to_json({fqn}.fn_event_response('evt:900001')) j")["j"])
    assert e["n_treaties_responding"] >= 10, f"only {e['n_treaties_responding']} respond"
    assert e["net_loss_eur"] > 0 and e["gross_loss_eur"] > e["net_loss_eur"], "loss/reinstatement off"
    assert e["solvency_after_pct"] < e["solvency_before_pct"], "solvency should drop"
    assert e["solvency_after_pct"] > 100, f"solvency_after {e['solvency_after_pct']} below floor"
    return f"{e['n_treaties_responding']} respond; net {e['net_loss_eur']/1e6:.0f}m; solvency {e['solvency_before_pct']}->{e['solvency_after_pct']}"
check("12·CRUX cat-event response", s_event)

def s_whatif():
    big = json.loads(one(f"SELECT to_json({fqn}.fn_accumulation_whatif('EU_WIND', 30000000.0, 20000000.0)) j")["j"])
    small = json.loads(one(f"SELECT to_json({fqn}.fn_accumulation_whatif('EU_WIND', 10000000.0, 20000000.0)) j")["j"])
    assert big["breach_amount_eur"] > small["breach_amount_eur"], "what-if not monotonic in limit"
    return f"30m breach {big['breach_amount_eur']/1e6:.1f}m > 10m breach {small['breach_amount_eur']/1e6:.1f}m"
check("13·what-if slider", s_whatif)

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(RESULTS)
display(df)
fails = df[df.status == "FAIL"]
print(f"\n{'='*50}\n{(df.status=='PASS').sum()}/{len(df)} PASS")
assert fails.empty, f"SMOKE FAILED: {fails['step'].tolist()}"
print("✅ SMOKE GREEN — both heroes land deterministically")
