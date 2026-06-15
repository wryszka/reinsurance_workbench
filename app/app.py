"""Bricksurance Re — Reinsurance Intelligence Workbench (thin app).

Presentation only. Every panel calls a real Databricks object (UC function / gold mart / serving endpoint /
Genie) and renders. No business logic, no scoring, no transformation here. The supervisor narration box calls
the supervisor endpoint through the USE_CACHE wrapper (narration only); structured panels never parse prose.
"""
import json, os, uuid
from fastapi import FastAPI
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from server import config, sql, agents

app = FastAPI(title="Reinsurance Intelligence Workbench")
DIST = os.path.join(os.path.dirname(__file__), "dist")


def _struct(fn_call: str):
    row = sql.query_one(f"SELECT to_json({fn_call}) AS r")
    return json.loads(row["r"]) if row and row.get("r") else {}


# ─────────────────────────── config ───────────────────────────
@app.get("/api/config")
def api_config():
    return {"catalog": config.CATALOG, "schema": config.SCHEMA, "use_cache": config.USE_CACHE,
            "workspace_host": config.workspace_host(), "genie_space_id": config.GENIE_SPACE_ID,
            "entity": "Bricksurance Re"}


# ─────────────────────────── CRO control tower ───────────────────────────
@app.get("/api/control-tower")
def control_tower():
    pos = sql.query(f"""SELECT zone_id, zone_name, peril, region, current_pml_1in200_eur, appetite_pml_1in200_eur,
                        headroom_eur, utilisation_pct, rag, n_treaties, market_pml_1in100_eur, market_pml_1in200_eur,
                        market_pml_1in250_eur FROM {config.fqn('gold_portfolio_position')} ORDER BY utilisation_pct DESC""")
    cap = sql.query(f"""SELECT DISTINCT diversified_bscr_eur, eligible_own_funds_eur, solvency_ratio_pct
                        FROM {config.fqn('gold_capital_position')}""")
    cat = sql.query(f"""SELECT zone_id, return_period, blended_pml_eur, vendor_min_pml_eur, vendor_max_pml_eur,
                        divergence_pct FROM {config.fqn('gold_cat_blended')} ORDER BY zone_id, return_period""")
    # most recent event (drives the live-event banner)
    ev = sql.query_one(f"""SELECT event_public_id, event_name, region, CAST(event_date AS STRING) event_date,
                           CAST(net_loss_eur AS double) net_loss_eur FROM {config.fqn('gold_event_response')}
                           ORDER BY event_date DESC LIMIT 1""")
    renewal = sql.query_one(f"""SELECT CAST(max(renewal_date) AS STRING) renewal_date,
                               sum(CASE WHEN status='new' THEN 1 ELSE 0 END) open_new,
                               count(*) total FROM {config.fqn('silver_submissions')}""") or {}
    return {"zones": pos, "capital": cap[0] if cap else {}, "cat_curves": cat,
            "live_event": ev, "renewal": renewal}


# ─────────────────────────── renewal desk (the daily flood) ───────────────────────────
@app.get("/api/submissions")
def submissions():
    rows = sql.query(f"""
        SELECT s.submission_public_id, s.cedant_name, s.broker, s.structure, s.lob, s.zone_name,
               s.proportional_or_xol, s.rol_pct, s.rating, s.data_quality_score, s.inbound_channel,
               s.is_cat_xol, s.is_peak, s.status, CAST(s.received_date AS STRING) received_date,
               CAST(s.renewal_date AS STRING) renewal_date
        FROM {config.fqn('silver_submissions')} s ORDER BY s.received_date DESC, s.submission_public_id""")
    # renewal-season banner counts (received in the last 7 days, still 'new')
    stats = sql.query_one(f"""SELECT
        count(*) total, sum(CASE WHEN status='new' THEN 1 ELSE 0 END) open_new,
        sum(CASE WHEN received_date >= date_sub(current_date(), 7) THEN 1 ELSE 0 END) this_week,
        max(renewal_date) renewal_date FROM {config.fqn('silver_submissions')}""") or {}
    return {"rows": rows, "stats": stats}


# ─────────────────────────── hero decision view (structured = real UC fns) ───────────────────────────
@app.get("/api/submission/{sid}/decision")
def decision(sid: str):
    s = sql.esc(sid)
    summary = _struct(f"{config.fqn('fn_submission_summary')}('{s}')")
    triage = _struct(f"{config.fqn('fn_triage_submission')}('{s}')")
    price = _struct(f"{config.fqn('fn_price_submission')}('{s}')")
    accumulation = _struct(f"{config.fqn('fn_accumulation_impact')}('{s}')")
    capital = _struct(f"{config.fqn('fn_capital_impact')}('{s}')")
    # The bind/refer/decline rule lives in Unity Catalog (fn_recommendation), not in the app.
    rec_struct = _struct(f"{config.fqn('fn_recommendation')}('{s}')")
    return {"summary": summary, "triage": triage, "price": price,
            "accumulation": accumulation, "capital": capital,
            "recommendation": rec_struct.get("recommendation", "refer"), "recommendation_detail": rec_struct}


@app.get("/api/submission/{sid}/alternative")
def alternative(sid: str):
    return {"alternative": _struct(f"{config.fqn('fn_portfolio_alternative')}('{sql.esc(sid)}')")}


@app.get("/api/submission/{sid}/counterparty")
def counterparty(sid: str):
    s = sql.esc(sid)
    return {"counterparty": sql.query_one(f"""
        SELECT c.cedant_name, c.rating, c.credit_quality_step, c.one_year_pd_pct, c.outlook,
               c.regulatory_watch, c.watch_note
        FROM {config.fqn('silver_submissions')} s
        JOIN {config.fqn('counterparties')} c ON s.cedant_id = c.cedant_id
        WHERE s.submission_public_id = '{s}' LIMIT 1""")}


@app.get("/api/submission/{sid}/whatif")
def whatif(sid: str, limit_eur: float = 30000000, attachment_eur: float = 20000000):
    s = sql.esc(sid)
    zone = sql.query_one(f"SELECT zone_id FROM {config.fqn('silver_submissions')} WHERE submission_public_id = '{s}' LIMIT 1")
    zid = zone["zone_id"] if zone else "EU_WIND"
    return _struct(f"{config.fqn('fn_accumulation_whatif')}('{sql.esc(zid)}', {float(limit_eur)}, {float(attachment_eur)})")


@app.get("/api/submission/{sid}/narrate")
def narrate(sid: str, role: str = "supervisor"):
    # The supervisor box is the REAL tool-calling agent — it calls the UC functions itself.
    if role == "supervisor":
        out = agents.ask_agent(f"Should we bind submission {sid}? Give the call with quantified reasons.",
                               custom_inputs={"submission_public_id": sid})
        return {"text": out.get("text", ""), "cache": out.get("cache"), "tools": out.get("tools", []),
                "endpoint": out.get("endpoint")}
    d = decision(sid)
    if role == "portfolio":
        d = {**d, "alternative": alternative(sid)["alternative"]}
    if role == "counterparty":
        d = {**d, "counterparty": counterparty(sid)["counterparty"]}
    role_substr = {"challenge": config.EP_CHALLENGE_SUBSTR, "dataquality": config.EP_DATAQUALITY_SUBSTR,
                   "portfolio": config.EP_PORTFOLIO_SUBSTR, "counterparty": config.EP_COUNTERPARTY_SUBSTR}.get(role, config.EP_CHALLENGE_SUBSTR)
    q = {"challenge": f"Argue the other side on {sid}.",
         "dataquality": f"Assess the data quality for {sid}.",
         "portfolio": f"Propose a diversifying alternative to {sid}.",
         "counterparty": f"Flag counterparty risk on {sid}."}.get(role, f"Comment on {sid}.")
    return agents.narrate(role_substr, q, d)


# ─────────────────────────── Reinsurance AI — ask the real tool-calling agent ───────────────────────────
@app.get("/api/agent/ask")
def agent_ask(q: str, sid: str = "", eid: str = ""):
    ci = {}
    if sid: ci["submission_public_id"] = sid
    if eid: ci["event_public_id"] = eid
    # interactive ask is always live (bypass cache) so the tool-call trace is real on screen
    out = agents.ask_agent(q, custom_inputs=ci, use_cache=False)
    return out


# ─────────────────────────── decision write-back (live audit capture) ───────────────────────────
@app.post("/api/decision")
def log_decision(payload: dict):
    sid = sql.esc(payload.get("submission_public_id", ""))
    action = sql.esc(payload.get("action", "refer"))   # recommend-to-bind | refer | decline
    who = sql.esc(payload.get("decided_by", "demo-underwriter"))
    bound = "true" if action == "recommend-to-bind" else "false"
    d = decision(payload.get("submission_public_id", ""))
    a, cp = d.get("accumulation", {}), d.get("capital", {})
    rid = "DEC-" + uuid.uuid4().hex[:8].upper()
    sql.query(f"""MERGE INTO {config.fqn('gov_decision_audit')} t
        USING (SELECT '{sid}' sid) s ON t.submission_public_id = s.sid
        WHEN MATCHED THEN UPDATE SET recommendation='{action}', decided_by='{who}', decision_ts=current_timestamp(), bound={bound}
        WHEN NOT MATCHED THEN INSERT (decision_id, submission_public_id, triage_decision, technical_verdict,
            breaches_appetite, breach_amount_eur, capital_destructive, rorac_pct, recommendation, decided_by, decision_ts, bound)
        VALUES ('{rid}','{sid}','{sql.esc(d.get('triage',{}).get('decision',''))}','{sql.esc(d.get('price',{}).get('verdict',''))}',
            {str(bool(a.get('breaches_appetite'))).lower()}, {float(a.get('breach_amount_eur') or 0)},
            {str(bool(cp.get('capital_destructive'))).lower()}, {float(cp.get('rorac_pct') or 0)},
            '{action}','{who}',current_timestamp(),{bound})""")
    return {"status": "logged", "decision_id": rid, "action": action}


# ─────────────────────────── cat event response (the wow) ───────────────────────────
@app.get("/api/events")
def events():
    return {"events": sql.query(f"""SELECT event_public_id, event_name, region, peril, return_period,
        CAST(industry_loss_eur AS double) industry_loss_eur, CAST(event_date AS STRING) event_date
        FROM {config.fqn('events')} ORDER BY event_date DESC""")}


@app.get("/api/event/{eid}")
def event_response(eid: str):
    return _struct(f"{config.fqn('fn_event_response')}('{sql.esc(eid)}')")


@app.get("/api/event/{eid}/treaties")
def event_treaties(eid: str):
    return {"treaties": sql.query(f"SELECT * FROM {config.fqn('fn_event_treaty_detail')}('{sql.esc(eid)}')")}


@app.get("/api/event/{eid}/narrate")
def event_narrate(eid: str):
    d = event_response(eid)
    return agents.narrate(config.EP_EVENT_SUBSTR, f"Brief the CRO on event {eid}.", d)


# ─────────────────────────── intake (ADEPT/CDR vs manual + quarantine) ───────────────────────────
@app.get("/api/intake")
def intake():
    channels = sql.query(f"""SELECT path, completeness_band, count(*) AS n
                             FROM {config.fqn('bronze_inbound_audit')} GROUP BY path, completeness_band ORDER BY path""")
    quarantine = sql.query(f"""SELECT submission_public_id, peril, quarantine_reason
                              FROM {config.fqn('bronze_quarantine_loss')}""")
    return {"channels": channels, "quarantine": quarantine}


# ─────────────────────────── ingestion (feed map + DQ scorecard + Document AI + geo) ───────────────────────────
_FEEDS = [
    ("Broker submissions (MRC slip)", "document", "ADEPT/CDR + manual", "bronze_mrc_submissions"),
    ("Premium bordereaux", "structured", "Cedant feed", "bronze_premium_bordereaux"),
    ("Loss bordereaux (files)", "file · schema-drift", "Cedant systems (CSV)", "bronze_bordereau_files"),
    ("Cedant exposure (geospatial)", "geospatial", "Cedant exposure", "bronze_exposure"),
    ("Cat vendor EP curves", "vendor", "3 cat-model vendors", "cat_vendor_curves"),
    ("Cat-event footprint", "streaming · JSON", "Vendor footprint feed", "bronze_event_footprint"),
    ("In-force book", "internal", "Internal book of record", "silver_inforce_treaties"),
]


@app.get("/api/ingestion/feeds")
def ingestion_feeds():
    # per-table DQ from the scorecard (avg pass rate); counts per feed table
    dq = {r["table_name"]: r for r in sql.query(f"""SELECT table_name, round(avg(pass_rate_pct),1) dq_pct,
          sum(failing_records) failing FROM {config.fqn('gold_dq_scorecard')} GROUP BY table_name""")}
    feeds = []
    for name, typ, src, tbl in _FEEDS:
        try:
            n = (sql.query_one(f"SELECT count(*) c FROM {config.fqn(tbl)}") or {}).get("c", "0")
        except Exception:
            n = "0"
        d = dq.get(tbl, {})
        feeds.append({"name": name, "type": typ, "source": src, "table": tbl, "rows": n,
                      "dq_pct": d.get("dq_pct"), "failing": d.get("failing"),
                      "status": ("amber" if d.get("dq_pct") is not None and float(d["dq_pct"]) < 98 else "green")})
    return {"feeds": feeds}


@app.get("/api/ingestion/scorecard")
def ingestion_scorecard():
    rows = sql.query(f"""SELECT layer, table_name, expectation, predicate, passing_records, failing_records,
        total_records, pass_rate_pct FROM {config.fqn('gold_dq_scorecard')} ORDER BY layer, table_name, expectation""")
    kpi = sql.query_one(f"""SELECT round(sum(passing_records)*100.0/greatest(sum(total_records),1),1) overall_pass,
        count(*) n_expectations, sum(CASE WHEN failing_records>0 THEN 1 ELSE 0 END) failing_checks,
        sum(failing_records) quarantined FROM {config.fqn('gold_dq_scorecard')}""") or {}
    return {"rules": rows, "kpi": kpi}


@app.get("/api/ingestion/quarantine")
def ingestion_quarantine():
    out = {}
    for label, q in [("loss bordereaux", f"SELECT submission_public_id id, peril detail, quarantine_reason reason FROM {config.fqn('bronze_quarantine_loss')}"),
                     ("MRC slips", f"SELECT submission_public_id id, structure detail, quarantine_reason reason FROM {config.fqn('bronze_quarantine_mrc')}"),
                     ("bordereaux files", f"SELECT submission_public_id id, _rescued_data detail, quarantine_reason reason FROM {config.fqn('bronze_quarantine_bordereaux')}")]:
        try:
            out[label] = sql.query(q)
        except Exception:
            out[label] = []
    return {"quarantine": out}


@app.get("/api/ingestion/document")
def ingestion_document():
    rows = sql.query(f"""SELECT submission_public_id, cedant, structure, perils, territories,
        CAST(layer_limit_eur AS double) layer_limit_eur, CAST(layer_attachment_eur AS double) layer_attachment_eur,
        CAST(subject_premium_eur AS double) subject_premium_eur, rol_pct, extraction_confidence,
        substr(slip_excerpt,1,900) slip_excerpt, source_file
        FROM {config.fqn('landing_mrc_extractions')} ORDER BY extraction_confidence DESC""")
    return {"extractions": rows}


@app.get("/api/ingestion/geo")
def ingestion_geo():
    return {"zones": sql.query(f"""SELECT zone_id, cresta, CAST(total_tiv_eur AS double) total_tiv_eur,
        n_locations, h3_cells, centroid_lat, centroid_lon FROM {config.fqn('gold_exposure_accumulation')}
        ORDER BY total_tiv_eur DESC""")}


# ─────────────────────────── governance ───────────────────────────
@app.get("/api/governance/inventory")
def gov_inventory():
    return {"inventory": sql.query(f"SELECT * FROM {config.fqn('gov_data_inventory')} ORDER BY sensitivity_tier"),
            "counterparties": sql.query(f"SELECT * FROM {config.fqn('gov_counterparty_checks')} ORDER BY cedant_id"),
            "solvency": sql.query(f"SELECT DISTINCT solvency_ratio_pct, diversified_bscr_eur, eligible_own_funds_eur, note FROM {config.fqn('gov_solvency_crosslink')}")}


@app.get("/api/governance/audit/{sid}")
def gov_audit(sid: str):
    return {"audit": sql.query(f"SELECT * FROM {config.fqn('fn_decision_audit')}('{sql.esc(sid)}')")}


@app.get("/api/governance/audit")
def gov_audit_all():
    return {"audit": sql.query(f"""SELECT decision_id, submission_public_id, recommendation, triage_decision,
        technical_verdict, breaches_appetite, capital_destructive, decided_by, CAST(decision_ts AS STRING) decision_ts, bound
        FROM {config.fqn('gov_decision_audit')} ORDER BY decision_ts DESC LIMIT 50""")}


# ─────────────────────────── real UC lineage + dynamic masking ───────────────────────────
@app.get("/api/governance/lineage")
def gov_lineage():
    cat, sch = config.CATALOG, config.SCHEMA
    # Real UC lineage captured automatically from the DLT pipeline (system.access.table_lineage).
    try:
        edges = sql.query(f"""SELECT DISTINCT source_table_name, target_table_name
            FROM system.access.table_lineage
            WHERE target_table_catalog='{cat}' AND target_table_schema='{sch}'
              AND source_table_name IS NOT NULL AND target_table_name IS NOT NULL
            LIMIT 200""")
        if edges:
            return {"source": "system.access.table_lineage (real UC lineage)", "edges": edges}
    except Exception as e:
        pass
    # Fallback: real UC objects from information_schema (nodes are real; edges = the DLT medallion DAG).
    tbls = sql.query(f"""SELECT table_name, table_type, comment FROM {cat}.information_schema.tables
        WHERE table_schema='{sch}' ORDER BY table_name""")
    return {"source": "information_schema.tables (real UC objects; edges = DLT medallion layers)",
            "tables": tbls,
            "layers": [["landing_*", "bronze_* (DLT)"], ["bronze_*", "silver_* (DLT)"],
                       ["silver_*", "gold_* (DLT)"], ["gold_*", "UC functions + serving"]]}


@app.get("/api/governance/masking")
def gov_masking():
    # the governed view applies a real UC column mask (is_account_group_member). The app SP isn't in the
    # privileged group, so it sees the redacted values — proving the mask is enforced by UC, not the app.
    rows = sql.query(f"SELECT cedant_name, rating, one_year_pd_pct, watch_note FROM {config.fqn('gov_counterparty_secure')} ORDER BY cedant_id LIMIT 8")
    return {"rows": rows,
            "rule": "fn mask_sensitive(v) → is_account_group_member('bricksurance_re_secret_readers') ? v : '*** restricted ***'",
            "note": "Enforced by Unity Catalog on the governed view gov_counterparty_secure. This app's service principal is outside the privileged group, so PD and watch notes are redacted here — by UC, not by the app."}


# ─────────────────────────── agents roster ───────────────────────────
@app.get("/api/agents")
def agent_roster():
    nodes = [
        {"kind": "supervisor", "name": "Reinsurance AI supervisor", "endpoint": config.resolve_endpoint(config.EP_SUPERVISOR_SUBSTR), "desc": "Composes the specialists into one recommendation. Narrates only — never binds."},
        {"kind": "agent", "name": "Cat-Event Response", "endpoint": config.resolve_endpoint(config.EP_EVENT_SUBSTR), "desc": "On a live event, briefs the CRO on book-wide loss, capital hit and the most exposed cedant."},
        {"kind": "agent", "name": "Portfolio Strategy", "endpoint": config.resolve_endpoint(config.EP_PORTFOLIO_SUBSTR), "desc": "Proposes a diversifying alternative when a deal saturates a peak zone."},
        {"kind": "agent", "name": "Counterparty Credit", "endpoint": config.resolve_endpoint(config.EP_COUNTERPARTY_SUBSTR), "desc": "Injects the cedant credit + regulatory-watch signal into the decision."},
        {"kind": "agent", "name": "Challenge / Second-Opinion", "endpoint": config.resolve_endpoint(config.EP_CHALLENGE_SUBSTR), "desc": "Argues the other side, quantified."},
        {"kind": "agent", "name": "Data Quality", "endpoint": config.resolve_endpoint(config.EP_DATAQUALITY_SUBSTR), "desc": "Bordereaux / exposure completeness narrative."},
        {"kind": "tool", "name": "fn_triage_submission", "desc": "Appetite decision (model)."},
        {"kind": "tool", "name": "fn_price_submission", "desc": "Reinsurance pricing — RoL / burning cost / combined ratio (model)."},
        {"kind": "tool", "name": "fn_accumulation_impact", "desc": "THE CRUX — marginal peak-zone PML vs appetite."},
        {"kind": "tool", "name": "fn_capital_impact", "desc": "THE CRUX — marginal SCR vs expected return (RoRAC)."},
        {"kind": "tool", "name": "fn_event_response", "desc": "Book-wide cat-event loss + capital impact in seconds."},
        {"kind": "genie", "name": "Ask the Portfolio", "desc": "AI/BI Genie over the gold marts.", "space": config.GENIE_SPACE_ID},
    ]
    return {"nodes": nodes, "tagline": "Models price → agents reason → experts review → every decision audited. Agents escalate; humans bind."}


# ─────────────────────────── reset (trigger job by substring) ───────────────────────────
@app.post("/api/reset")
def reset():
    try:
        w = config.get_workspace_client()
        job = next((j for j in w.jobs.list() if "reinsurance_99_reset" in (j.settings.name or "")), None)
        if not job:
            return JSONResponse({"error": "reset job not found"}, status_code=404)
        run = w.jobs.run_now(job_id=job.job_id)
        return {"status": "triggered", "run_id": run.run_id}
    except Exception as e:
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


# ─────────────────────────── static SPA ───────────────────────────
if os.path.isdir(DIST):
    app.mount("/assets", StaticFiles(directory=os.path.join(DIST, "assets")), name="assets") if os.path.isdir(os.path.join(DIST, "assets")) else None


@app.get("/")
def root():
    return FileResponse(os.path.join(DIST, "index.html"))


@app.get("/healthz")
def healthz():
    return {"ok": True}
