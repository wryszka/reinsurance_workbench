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


def _struct_sql(fn_call: str) -> str:
    return f"SELECT to_json({fn_call}) AS r"


def _parse_struct(rows) -> dict:
    row = rows[0] if rows else None
    return json.loads(row["r"]) if row and row.get("r") else {}


# ─────────────────────────── config ───────────────────────────
@app.get("/api/config")
def api_config():
    host = config.workspace_host()
    if host and not host.startswith("http"):
        host = "https://" + host
    dash = f"{host}/dashboardsv3/{config.DASHBOARD_ID}/published" if (host and config.DASHBOARD_ID) else ""
    dash_embed = f"{host}/embed/dashboardsv3/{config.DASHBOARD_ID}" if (host and config.DASHBOARD_ID) else ""
    return {"catalog": config.CATALOG, "schema": config.SCHEMA, "use_cache": config.USE_CACHE,
            "workspace_host": config.workspace_host(), "genie_space_id": config.GENIE_SPACE_ID,
            "dashboard_url": dash, "dashboard_embed_url": dash_embed, "entity": "Bricksurance Re"}


# ─────────────────────────── CRO control tower ───────────────────────────
@app.get("/api/control-tower")
def control_tower():
    r = sql.query_many({
        "pos": f"""SELECT zone_id, zone_name, peril, region, current_pml_1in200_eur, appetite_pml_1in200_eur,
                   headroom_eur, utilisation_pct, rag, n_treaties, market_pml_1in100_eur, market_pml_1in200_eur,
                   market_pml_1in250_eur FROM {config.fqn('gold_portfolio_position')} ORDER BY utilisation_pct DESC""",
        "cap": f"""SELECT DISTINCT diversified_bscr_eur, eligible_own_funds_eur, solvency_ratio_pct
                   FROM {config.fqn('gold_capital_position')}""",
        "cat": f"""SELECT zone_id, return_period, blended_pml_eur, vendor_min_pml_eur, vendor_max_pml_eur,
                   divergence_pct FROM {config.fqn('gold_cat_blended')} ORDER BY zone_id, return_period""",
        # most recent event (drives the live-event banner)
        "ev": f"""SELECT event_public_id, event_name, region, CAST(event_date AS STRING) event_date,
                  CAST(net_loss_eur AS double) net_loss_eur FROM {config.fqn('gold_event_response')}
                  ORDER BY event_date DESC LIMIT 1""",
        "renewal": f"""SELECT CAST(max(renewal_date) AS STRING) renewal_date,
                       sum(CASE WHEN status='new' THEN 1 ELSE 0 END) open_new,
                       count(*) total FROM {config.fqn('silver_submissions')}""",
        # provenance: when the gold marts were built by the pipeline + when this request hit the warehouse
        "meta": f"""SELECT CAST(current_timestamp() AS STRING) queried_at,
                    CAST(max(_gold_built_at) AS STRING) built_at FROM {config.fqn('gold_portfolio_position')}""",
    })
    return {"zones": r["pos"], "capital": sql.first(r["cap"]) or {}, "cat_curves": r["cat"],
            "live_event": sql.first(r["ev"]), "renewal": sql.first(r["renewal"]) or {},
            "meta": sql.first(r["meta"]) or {}}


# Drill-downs that prove the tiles are computed, not hardcoded: a zone PML is the SUM of its in-force
# treaty contributions; the cat number sits between the three vendor curves; the solvency ratio is the
# capital build. Each returns the constituent rows + source tables + build/query timestamps.
@app.get("/api/control-tower/zone/{zone_id}")
def control_tower_zone(zone_id: str):
    z = sql.esc(zone_id)
    r = sql.query_many({
        "zone": f"""SELECT zone_id, zone_name, peril, region, current_pml_1in200_eur, appetite_pml_1in200_eur,
                    headroom_eur, utilisation_pct, rag, n_treaties FROM {config.fqn('gold_portfolio_position')}
                    WHERE zone_id='{z}'""",
        "treaties": f"""SELECT treaty_id, cedant_id, lob, structure, attachment_eur, limit_eur,
                        ceded_premium_eur, expected_loss_eur, modeled_pml_1in200_contrib_eur, is_correlated_ref
                        FROM {config.fqn('inforce_treaties')} WHERE zone_id='{z}'
                        ORDER BY modeled_pml_1in200_contrib_eur DESC""",
        "total": f"""SELECT count(*) n, sum(modeled_pml_1in200_contrib_eur) total_eur,
                     sum(ceded_premium_eur) prem_eur FROM {config.fqn('inforce_treaties')} WHERE zone_id='{z}'""",
        "vendors": f"""SELECT vendor, pml_eur FROM {config.fqn('cat_vendor_curves')}
                       WHERE zone_id='{z}' AND return_period=200 ORDER BY vendor""",
        "blended": f"""SELECT blended_pml_eur, vendor_min_pml_eur, vendor_max_pml_eur, divergence_pct, n_vendors
                       FROM {config.fqn('gold_cat_blended')} WHERE zone_id='{z}' AND return_period=200""",
        "meta": f"""SELECT CAST(current_timestamp() AS STRING) queried_at,
                    CAST(max(_gold_built_at) AS STRING) built_at FROM {config.fqn('gold_portfolio_position')}""",
    })
    return {"zone": sql.first(r["zone"]) or {}, "treaties": r["treaties"], "total": sql.first(r["total"]) or {},
            "vendors": r["vendors"], "blended": sql.first(r["blended"]) or {}, "meta": sql.first(r["meta"]) or {}}


@app.get("/api/control-tower/capital")
def control_tower_capital():
    r = sql.query_many({
        "zones": f"""SELECT zone_name, current_pml_1in200_eur, standalone_scr_eur
                     FROM {config.fqn('gold_capital_position')} ORDER BY standalone_scr_eur DESC""",
        "agg": f"""SELECT DISTINCT sum_standalone_scr_eur, diversification_benefit_pct, diversified_bscr_eur,
                   eligible_own_funds_eur, solvency_ratio_pct FROM {config.fqn('gold_capital_position')}""",
        "meta": f"""SELECT CAST(current_timestamp() AS STRING) queried_at,
                    CAST(max(_gold_built_at) AS STRING) built_at FROM {config.fqn('gold_capital_position')}""",
    })
    return {"zones": r["zones"], "agg": sql.first(r["agg"]) or {}, "meta": sql.first(r["meta"]) or {}}


# ─────────────────────────── renewal desk (the daily flood) ───────────────────────────
@app.get("/api/submissions")
def submissions():
    r = sql.query_many({
        "rows": f"""
        SELECT s.submission_public_id, s.cedant_name, s.broker, s.structure, s.lob, s.zone_name,
               s.proportional_or_xol, s.rol_pct, s.rating, s.data_quality_score, s.inbound_channel,
               s.is_cat_xol, s.is_peak, s.status, CAST(s.received_date AS STRING) received_date,
               CAST(s.renewal_date AS STRING) renewal_date
        FROM {config.fqn('silver_submissions')} s ORDER BY s.received_date DESC, s.submission_public_id""",
        # renewal-season banner counts (received in the last 7 days, still 'new')
        "stats": f"""SELECT
        count(*) total, sum(CASE WHEN status='new' THEN 1 ELSE 0 END) open_new,
        sum(CASE WHEN received_date >= date_sub(current_date(), 7) THEN 1 ELSE 0 END) this_week,
        max(renewal_date) renewal_date FROM {config.fqn('silver_submissions')}""",
    })
    return {"rows": r["rows"], "stats": sql.first(r["stats"]) or {}}


# ─────────────────────────── hero decision view (structured = real UC fns) ───────────────────────────
@app.get("/api/submission/{sid}/decision")
def decision(sid: str):
    s = sql.esc(sid)
    # The bind/refer/decline rule lives in Unity Catalog (fn_recommendation), not in the app.
    # All six UC functions are independent → fire them concurrently (was 6 sequential round-trips).
    r = sql.query_many({
        "summary": _struct_sql(f"{config.fqn('fn_submission_summary')}('{s}')"),
        "triage": _struct_sql(f"{config.fqn('fn_triage_submission')}('{s}')"),
        "price": _struct_sql(f"{config.fqn('fn_price_submission')}('{s}')"),
        "accumulation": _struct_sql(f"{config.fqn('fn_accumulation_impact')}('{s}')"),
        "capital": _struct_sql(f"{config.fqn('fn_capital_impact')}('{s}')"),
        "rec": _struct_sql(f"{config.fqn('fn_recommendation')}('{s}')"),
    })
    rec_struct = _parse_struct(r["rec"])
    return {"summary": _parse_struct(r["summary"]), "triage": _parse_struct(r["triage"]),
            "price": _parse_struct(r["price"]), "accumulation": _parse_struct(r["accumulation"]),
            "capital": _parse_struct(r["capital"]),
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


def _uc(cache):
    # per-request cache override from the UI toggle (cache=1/0); falls back to the env default.
    return config.USE_CACHE if cache is None else bool(cache)


@app.get("/api/submission/{sid}/narrate")
def narrate(sid: str, role: str = "supervisor", cache: int = None):
    uc = _uc(cache)
    # The supervisor box is the REAL tool-calling agent — it calls the UC functions itself.
    if role == "supervisor":
        out = agents.ask_agent(f"Should we bind submission {sid}? Give the call with quantified reasons.",
                               custom_inputs={"submission_public_id": sid}, use_cache=uc)
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
    return agents.narrate(role_substr, q, d, use_cache=uc)


# ─────────────────────────── Reinsurance AI — ask the real tool-calling agent ───────────────────────────
@app.get("/api/agent/ask")
def agent_ask(q: str, sid: str = "", eid: str = "", cache: int = None):
    ci = {}
    if sid: ci["submission_public_id"] = sid
    if eid: ci["event_public_id"] = eid
    # honours the AI-cache toggle: cached = instant on stage; live = runs the agent and shows the trace.
    out = agents.ask_agent(q, custom_inputs=ci, use_cache=_uc(cache))
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
def event_narrate(eid: str, cache: int = None):
    d = event_response(eid)
    return agents.narrate(config.EP_EVENT_SUBSTR, f"Brief the CRO on event {eid}.", d, use_cache=_uc(cache))


# ─────────────────────────── intake (ADEPT/CDR vs manual + quarantine) ───────────────────────────
@app.get("/api/intake")
def intake():
    r = sql.query_many({
        "channels": f"""SELECT path, completeness_band, count(*) AS n
                        FROM {config.fqn('bronze_inbound_audit')} GROUP BY path, completeness_band ORDER BY path""",
        "quarantine": f"""SELECT submission_public_id, peril, quarantine_reason
                          FROM {config.fqn('bronze_quarantine_loss')}""",
    })
    return {"channels": r["channels"], "quarantine": r["quarantine"]}


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


# ─────────── Cat-model output at scale — a YELT in Delta, queried against the live book ───────────
@app.get("/api/yelt/summary")
def yelt_summary():
    y = config.fqn("cat_yelt")
    r = sql.query_many({
        "stat": f"SELECT count(*) rows, count(DISTINCT trial_id) trials, count(DISTINCT event_id) events, "
                f"max(trial_id) n_years FROM {y}",
        "byzone": f"SELECT zone_id, peril, region, count(*) n_losses, "
                  f"CAST(round(avg(gross_loss_eur)) AS double) avg_loss, CAST(max(gross_loss_eur) AS double) max_loss "
                  f"FROM {y} GROUP BY zone_id, peril, region ORDER BY n_losses DESC",
    })
    size = {}
    try:
        d = sql.query_one(f"DESCRIBE DETAIL {y}")
        if d:
            size = {"size_bytes": d.get("sizeInBytes"), "num_files": d.get("numFiles")}
    except Exception:
        pass
    return {"stat": sql.first(r["stat"]) or {}, "byzone": r["byzone"], "size": size, "table": y}


@app.get("/api/yelt/ep")
def yelt_ep():
    import time as _t
    y = config.fqn("cat_yelt"); t = config.fqn("inforce_treaties")
    n = int((sql.query_one(f"SELECT max(trial_id) m FROM {y}") or {}).get("m") or 100000)
    ep_sql = f"""
    WITH zone_layer AS (
      SELECT zone_id, SUM(limit_eur) AS zone_limit,
             CAST(ROUND(SUM(attachment_eur*limit_eur)/SUM(limit_eur)) AS BIGINT) AS zone_attach
      FROM {t} WHERE structure = 'Cat XoL' GROUP BY zone_id),
    ev AS (
      SELECT y.trial_id,
             LEAST(GREATEST(y.gross_loss_eur - z.zone_attach, 0), z.zone_limit) AS ceded_eur
      FROM {y} y JOIN zone_layer z USING (zone_id)),
    by_year AS (SELECT trial_id, SUM(ceded_eur) AS aep, MAX(ceded_eur) AS oep FROM ev GROUP BY trial_id),
    all_trials AS (SELECT explode(sequence(1, {n})) AS trial_id),
    dens AS (SELECT a.trial_id, COALESCE(b.aep, 0) AS aep, COALESCE(b.oep, 0) AS oep
             FROM all_trials a LEFT JOIN by_year b USING (trial_id))
    SELECT CAST(percentile(oep, 0.99) AS double) oep100, CAST(percentile(oep, 0.995) AS double) oep200,
           CAST(percentile(oep, 0.996) AS double) oep250, CAST(percentile(aep, 0.99) AS double) aep100,
           CAST(percentile(aep, 0.995) AS double) aep200, CAST(percentile(aep, 0.996) AS double) aep250,
           CAST(avg(aep) AS double) aal, CAST(max(oep) AS double) worst FROM dens"""
    t0 = _t.time(); row = sql.query_one(ep_sql); ms = int((_t.time() - t0) * 1000)
    zone_sql = f"""
    WITH zl AS (SELECT zone_id, SUM(limit_eur) zlim, CAST(ROUND(SUM(attachment_eur*limit_eur)/SUM(limit_eur)) AS BIGINT) zatt
                FROM {t} WHERE structure = 'Cat XoL' AND zone_id IN (SELECT DISTINCT zone_id FROM {y}) GROUP BY zone_id),
    ev AS (SELECT y.zone_id, y.trial_id, LEAST(GREATEST(y.gross_loss_eur - z.zatt, 0), z.zlim) ceded
           FROM {y} y JOIN zl z USING (zone_id)),
    ymax AS (SELECT zone_id, trial_id, MAX(ceded) oep FROM ev GROUP BY zone_id, trial_id),
    zlist AS (SELECT DISTINCT zone_id FROM zl),
    allt AS (SELECT zone_id, explode(sequence(1, {n})) trial_id FROM zlist),
    dens AS (SELECT a.zone_id, COALESCE(m.oep, 0) oep FROM allt a LEFT JOIN ymax m USING (zone_id, trial_id))
    SELECT zone_id, CAST(percentile(oep, 0.995) AS double) oep200, CAST(avg(oep) AS double) mean_oep
    FROM dens GROUP BY zone_id ORDER BY oep200 DESC"""
    return {"ep": row or {}, "elapsed_ms": ms, "n_trials": n, "zones": sql.query(zone_sql), "table": y}


# ─────────── Ask the Portfolio — real AI/BI Genie, surfaced in-app ───────────
GENIE_EXAMPLES = [
    "Which peak zone is closest to its appetite right now?",
    "What is our European windstorm 1-in-200 PML versus appetite?",
    "How many in-force treaties do we have by peak zone?",
    "What is the current Solvency II ratio and diversified BSCR?",
    "Which cedant has the largest ceded premium?",
]


@app.get("/api/genie/examples")
def genie_examples():
    return {"examples": GENIE_EXAMPLES, "space": config.GENIE_SPACE_ID,
            "title": "Ask the Portfolio", "configured": bool(config.GENIE_SPACE_ID)}


@app.get("/api/genie/ask")
def genie_ask(q: str):
    space = config.GENIE_SPACE_ID
    if not space:
        return {"error": "Genie space not configured."}
    try:
        w = config.get_workspace_client()
        msg = w.genie.start_conversation_and_wait(space, q)
        text, sql_text, cols, rows = "", "", [], []
        for a in (msg.attachments or []):
            if getattr(a, "text", None) and a.text and a.text.content:
                text = a.text.content
            if getattr(a, "query", None) and a.query:
                sql_text = a.query.query or ""
        try:
            qr = w.genie.get_message_query_result(space, msg.conversation_id, msg.id)
            sr = getattr(qr, "statement_response", None)
            if sr and sr.result and sr.manifest:
                cols = [c.name for c in sr.manifest.schema.columns]
                rows = (sr.result.data_array or [])[:25]
        except Exception:
            pass
        return {"text": text, "sql": sql_text, "columns": cols, "rows": rows, "space": space}
    except Exception as e:
        return {"error": str(e)[:200]}


# ─────────── source catalogue (the full ingestion picture; new sources are MOCK, expandable) ───────────
# status: live = a real feed in this demo (row count + DQ pulled); mock = illustrative, not yet wired; roadmap = pattern.
_SOURCE_GROUPS = [
    {"group": "Submission intake", "icon": "📨", "why": "Turn what brokers send into structured, priceable risk.", "sources": [
        {"name": "MRC slips", "method": "Auto Loader + ai_query (Document AI)", "cadence": "event-driven", "status": "live", "table": "bronze_mrc_submissions", "note": "Unstructured slips → fields; low-confidence quarantined."},
        {"name": "ADEPT / CDR structured feed", "method": "Auto Loader (cloudFiles)", "cadence": "daily", "status": "live", "table": "bronze_submissions", "note": "Clean market-standard submission feed."},
        {"name": "Broker email / portal", "method": "Mailbox → Volume + Document AI", "cadence": "event-driven", "status": "mock", "note": "Inbound attachments auto-read."}]},
    {"group": "Pricing & model build", "icon": "📊", "why": "The history and exposure that train the pricing & triage models.", "sources": [
        {"name": "Premium bordereaux", "method": "Auto Loader (schema rescue)", "cadence": "monthly/quarterly", "status": "live", "table": "bronze_premium_bordereaux"},
        {"name": "Loss bordereaux", "method": "Auto Loader (schema-drift rescue)", "cadence": "monthly/quarterly", "status": "live", "table": "bronze_loss_bordereaux"},
        {"name": "Exposure / SOV (locations, TIV)", "method": "read_files + h3", "cadence": "per renewal", "status": "live", "table": "bronze_exposure"},
        {"name": "Cat vendor EP curves (ELT/YLT)", "method": "read_files (Parquet)", "cadence": "vendor release", "status": "live", "table": "cat_vendor_curves"},
        {"name": "Historical event catalogue (stochastic set)", "method": "read_files (Parquet)", "cadence": "annual", "status": "mock"},
        {"name": "Industry loss indices (PCS / PERILS)", "method": "REST API → Delta", "cadence": "per event", "status": "mock"},
        {"name": "Claims / social-inflation indices", "method": "REST API → Delta", "cadence": "monthly", "status": "mock"}]},
    {"group": "Live event & real-time response", "icon": "🌀", "why": "Know the storm has hit — and react in minutes, not days.", "sources": [
        {"name": "Windstorm tracks & footprints (ECMWF · Met Office · DWD)", "method": "Structured Streaming (Kafka)", "cadence": "streaming · minutes", "status": "live", "table": "bronze_event_footprint", "note": "Drives the live storm radar above."},
        {"name": "Real-time wind-speed grids", "method": "Auto Loader (continuous, GRIB→Delta)", "cadence": "streaming", "status": "mock"},
        {"name": "Vendor real-time event feeds (RMS HWind · Verisk Respond)", "method": "REST / stream → Delta", "cadence": "per event", "status": "mock"},
        {"name": "Flood gauges & river levels", "method": "Structured Streaming (Kinesis)", "cadence": "streaming · minutes", "status": "mock"},
        {"name": "Earthquake feed (USGS)", "method": "REST API → Delta", "cadence": "streaming", "status": "mock"},
        {"name": "Wildfire hotspots (NASA FIRMS)", "method": "REST API → Delta", "cadence": "streaming", "status": "mock"},
        {"name": "Satellite / aerial imagery", "method": "Volumes + ai_query (vision)", "cadence": "per event", "status": "mock"}]},
    {"group": "Counterparty & compliance", "icon": "🛡️", "why": "Who we're trading with — and whether we're allowed to.", "sources": [
        {"name": "Credit ratings (S&P / AM Best)", "method": "REST API → Delta", "cadence": "daily", "status": "mock"},
        {"name": "Sanctions / watchlists (OFAC · EU · UK)", "method": "List ingest + screen", "cadence": "daily", "status": "mock"},
        {"name": "ESG / climate-risk scores", "method": "REST API → Delta", "cadence": "quarterly", "status": "mock"}]},
    {"group": "Reference & enrichment", "icon": "🗺️", "why": "The shared dimensions everything joins to.", "sources": [
        {"name": "FX rates", "method": "REST API → Delta", "cadence": "daily", "status": "mock"},
        {"name": "CRESTA / admin boundaries", "method": "read_files (geo)", "cadence": "annual", "status": "mock"},
        {"name": "Peril & coverage reference", "method": "read_files", "cadence": "ad-hoc", "status": "mock"}]},
]


@app.get("/api/ingestion/sources")
def ingestion_sources():
    dq = {r["table_name"]: r for r in sql.query(f"""SELECT table_name, round(avg(pass_rate_pct),1) dq_pct
          FROM {config.fqn('gold_dq_scorecard')} GROUP BY table_name""")}
    groups = []
    n_live = n_mock = 0
    for g in _SOURCE_GROUPS:
        srcs = []
        for s in g["sources"]:
            row = dict(s)
            if s["status"] == "live":
                n_live += 1
                if s.get("table"):
                    try:
                        row["rows"] = (sql.query_one(f"SELECT count(*) c FROM {config.fqn(s['table'])}") or {}).get("c")
                    except Exception:
                        row["rows"] = None
                    d = dq.get(s["table"], {})
                    row["dq_pct"] = d.get("dq_pct")
            else:
                n_mock += 1
            srcs.append(row)
        groups.append({**g, "sources": srcs})
    return {"groups": groups, "n_live": n_live, "n_mock": n_mock,
            "note": "Live = a real feed in this demo (row count + DQ shown). Mock = illustrative, ready to wire. The platform pattern (Auto Loader, Structured Streaming, read_files, ai_query, Volumes) is identical for all of them."}


@app.get("/api/ingestion/storm")
def ingestion_storm():
    # MOCK near-real-time windstorm track (Windstorm Eckhart) — lands on the same net loss as the Cat Event page.
    return {"event": "Windstorm Eckhart", "region": "NW Europe", "feed": "ECMWF/Met-Office footprint via Kafka → Auto Loader",
            "frames": [
                {"t": "T-36h", "phase": "Forming", "wind_kph": 90, "exposed_tiv_eur": 0, "exposed_cedants": 0, "modelled_net_eur": 0, "note": "Vendor track feed picks up a developing Atlantic low — no exposure in the cone yet."},
                {"t": "T-18h", "phase": "Intensifying", "wind_kph": 140, "exposed_tiv_eur": 2.1e9, "exposed_cedants": 4, "modelled_net_eur": 0, "note": "Footprint cone overlaps Benelux / NW-France CRESTA zones — exposure flagged."},
                {"t": "T-6h", "phase": "Approaching landfall", "wind_kph": 165, "exposed_tiv_eur": 4.8e9, "exposed_cedants": 11, "modelled_net_eur": 38e6, "note": "Wind field firms up — first modelled loss on the exposed windstorm XoL layers."},
                {"t": "T-0", "phase": "Landfall", "wind_kph": 175, "exposed_tiv_eur": 6.2e9, "exposed_cedants": 18, "modelled_net_eur": 133e6, "fire_event": True, "note": "Landfall. 22 treaties respond — the book-wide Cat Event response fires automatically."},
                {"t": "T+12h", "phase": "Post-event", "wind_kph": 120, "exposed_tiv_eur": 6.2e9, "exposed_cedants": 18, "modelled_net_eur": 131e6, "note": "Footprint refines; modelled loss stabilises. Reinstatement notices issued."},
                {"t": "T+2w", "phase": "Cedant reports", "wind_kph": 0, "exposed_tiv_eur": 6.2e9, "exposed_cedants": 18, "modelled_net_eur": 131e6, "note": "Cedant-reported losses begin replacing the day-one modelled view as claims develop."}]}


# ─────────────────────────── governance ───────────────────────────
@app.get("/api/governance/inventory")
def gov_inventory():
    r = sql.query_many({
        "inventory": f"SELECT * FROM {config.fqn('gov_data_inventory')} ORDER BY sensitivity_tier",
        "counterparties": f"SELECT * FROM {config.fqn('gov_counterparty_checks')} ORDER BY cedant_id",
        "solvency": f"SELECT DISTINCT solvency_ratio_pct, diversified_bscr_eur, eligible_own_funds_eur, note FROM {config.fqn('gov_solvency_crosslink')}",
    })
    return {"inventory": r["inventory"], "counterparties": r["counterparties"], "solvency": r["solvency"]}


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


# ─────────────────────────── model governance (real MLflow registry) ───────────────────────────
def _ms_to_date(ms):
    try:
        import datetime
        return datetime.datetime.utcfromtimestamp(int(ms) / 1000).strftime("%Y-%m-%d")
    except Exception:
        return None


@app.get("/api/governance/models")
def gov_models():
    w = config.get_workspace_client()
    cat, sch = config.CATALOG, config.SCHEMA
    out = []
    for nm, label, ep, kind in [
        ("model_triage_classifier", "Triage classifier", "reinsurance-triage", "Appetite decision — in/out, fast-track vs refer"),
        ("model_loss_ratio", "Pricing — loss-ratio / burning cost", "reinsurance-pricing", "Technical price — predicted loss ratio (not a GLM)")]:
        full = f"{cat}.{sch}.{nm}"
        try:
            vers = list(w.model_versions.list(full_name=full))
            latest = max((int(v.version) for v in vers), default=None)
            champ = None
            try:
                champ = str(w.model_versions.get_by_alias(full_name=full, alias="champion").version)
            except Exception:
                champ = str(latest) if latest else None
            created = next((_ms_to_date(getattr(v, "created_at", None)) for v in vers if str(v.version) == str(latest)), None)
            out.append({"name": nm, "full_name": full, "label": label, "role": kind,
                        "endpoint": config.resolve_endpoint(ep), "versions": len(vers),
                        "latest_version": latest, "champion_version": champ, "created_at": created, "status": "READY"})
        except Exception as e:
            out.append({"name": nm, "label": label, "role": kind, "error": str(e)[:120]})
    return {"models": out,
            "note": "Registered in the Unity Catalog Model Registry and served scale-to-zero via Mosaic AI Model Serving. The @champion alias is the exact version the pricing and triage cards call — so a decision can always be tied to the model version that produced it."}


# ─────────────────────────── AI activity / agent-reasoning audit ───────────────────────────
@app.get("/api/governance/ai-activity")
def gov_ai_activity_route(id: str = ""):
    where = f"WHERE subject_id='{sql.esc(id)}'" if id else ""
    return {"activity": sql.query(f"""SELECT subject_id, subject_kind, agent_name, agent_role, tools_used, signal,
        reasoning_text, CAST(created_ts AS STRING) created_ts FROM {config.fqn('gov_ai_activity')} {where}
        ORDER BY subject_id, activity_id""")}


# ─────────────────────────── deal track — full governed lifecycle of one deal ───────────────────────────
def _m(v):
    try:
        return "€" + format(round(float(v) / 1e6, 1), ",") + "m"
    except Exception:
        return "—"


@app.get("/api/governance/track")
def gov_track(id: str):
    s = sql.esc(id)
    agents = sql.query(f"""SELECT agent_name, signal, tools_used, reasoning_text FROM {config.fqn('gov_ai_activity')}
        WHERE subject_id='{s}' ORDER BY activity_id""")
    if id.startswith("evt:"):
        ev = _struct(f"{config.fqn('fn_event_response')}('{s}')")
        if not ev:
            return {"found": False, "id": id}
        stages = [
            {"stage": "Footprint ingested", "status": "done", "source": "bronze_event_footprint",
             "detail": f"{ev.get('event_name')} — {ev.get('region')}, 1-in-{ev.get('return_period')}. Vendor event footprint ingested as JSON."},
            {"stage": "Treaties matched", "status": "done", "source": "fn_event_treaty_detail",
             "detail": f"{ev.get('n_treaties_responding')} in-force treaties respond, by attachment, limit and footprint."},
            {"stage": "Loss computed", "status": "done", "source": "fn_event_response",
             "detail": f"Gross {_m(ev.get('gross_loss_eur'))}, − reinstatement {_m(ev.get('reinstatement_premium_eur'))} = net {_m(ev.get('net_loss_eur'))}. Most exposed: {ev.get('top_cedant')} {_m(ev.get('top_cedant_loss_eur'))}."},
            {"stage": "Capital impact", "status": "done", "source": "fn_event_response + gold_capital_position",
             "detail": f"Solvency II {ev.get('solvency_before_pct')}% → {ev.get('solvency_after_pct')}% — above the 100% floor."},
            {"stage": "CRO briefed", "status": "done", "source": "Cat-Event agent",
             "detail": "Book-wide response assembled and briefed in seconds."},
        ]
        return {"found": True, "kind": "event", "title": ev.get("event_name", id),
                "subtitle": f"{ev.get('region','')} · 1-in-{ev.get('return_period','')} · net {_m(ev.get('net_loss_eur'))}",
                "stages": stages, "agents": agents, "decision": None}
    # submission lifecycle
    sub = sql.query_one(f"""SELECT submission_public_id, cedant_name, broker, structure, zone_name, inbound_channel,
        CAST(received_date AS STRING) received_date FROM {config.fqn('silver_submissions')} WHERE submission_public_id='{s}'""")
    if not sub:
        return {"found": False, "id": id}
    ext = sql.query_one(f"""SELECT extraction_confidence, source_file FROM {config.fqn('landing_mrc_extractions')}
        WHERE submission_public_id='{s}'""")
    r = sql.query_many({
        "tri": _struct_sql(f"{config.fqn('fn_triage_submission')}('{s}')"),
        "pri": _struct_sql(f"{config.fqn('fn_price_submission')}('{s}')"),
        "acc": _struct_sql(f"{config.fqn('fn_accumulation_impact')}('{s}')"),
        "cap": _struct_sql(f"{config.fqn('fn_capital_impact')}('{s}')"),
    })
    tri, pri, acc, cap = (_parse_struct(r["tri"]), _parse_struct(r["pri"]), _parse_struct(r["acc"]), _parse_struct(r["cap"]))
    dec = sql.query_one(f"""SELECT recommendation, decided_by, CAST(decision_ts AS STRING) decision_ts, bound
        FROM {config.fqn('gov_decision_audit')} WHERE submission_public_id='{s}'""")
    manual = (sub.get("inbound_channel") != "ADEPT_CDR")
    breach = acc.get("breaches_appetite")
    stages = [
        {"stage": "Submission arrived", "status": "done", "source": "silver_submissions",
         "detail": f"{sub.get('cedant_name')} via {sub.get('broker')} — {sub.get('structure')} in {sub.get('zone_name')}. Channel: {'manual slip' if manual else 'ADEPT/CDR clean feed'}, received {sub.get('received_date')}."},
    ]
    if ext:
        conf = ext.get("extraction_confidence")
        stages.append({"stage": "Document AI extraction", "status": ("done" if (conf and float(conf) >= 0.75) else "missing"),
                       "source": "landing_mrc_extractions",
                       "detail": f"Slip read by ai_query at confidence {conf} ({'passed the 0.75 gate' if (conf and float(conf) >= 0.75) else 'below gate → quarantined'})."})
    stages += [
        {"stage": "Triage", "status": "done", "source": "fn_triage_submission",
         "detail": f"{(tri.get('decision') or '').replace('_',' ')}" + (f" · {tri.get('confidence')}% confidence" if tri.get('confidence') is not None else "")},
        {"stage": "Price", "status": "done", "source": "fn_price_submission",
         "detail": f"{pri.get('verdict','—')}" + (f" · combined { _pctf(pri.get('combined_ratio_pct')) }" if pri.get('combined_ratio_pct') is not None else "")},
        {"stage": "Accumulation (the crux)", "status": ("missing" if breach else "done"), "source": "fn_accumulation_impact",
         "detail": (f"BREACH — +{_m(acc.get('marginal_pml_1in200_eur'))} marginal PML pushes {acc.get('zone_name')} past appetite by {_m(acc.get('breach_amount_eur'))}." if breach
                    else f"OK — marginal {_m(acc.get('marginal_pml_1in200_eur'))}, within appetite.")},
        {"stage": "Capital (the crux)", "status": ("missing" if cap.get("capital_destructive") else "done"), "source": "fn_capital_impact",
         "detail": (f"DESTRUCTIVE — RoRAC {_pctf(cap.get('rorac_pct'))} below the {_pctf(cap.get('hurdle_pct'))} hurdle." if cap.get("capital_destructive")
                    else f"ACCRETIVE — RoRAC {_pctf(cap.get('rorac_pct'))} above the {_pctf(cap.get('hurdle_pct'))} hurdle.")},
    ]
    if dec:
        stages.append({"stage": "Decision logged", "status": "done", "source": "gov_decision_audit",
                       "detail": f"{(dec.get('recommendation') or '').upper()} by {dec.get('decided_by')} at {dec.get('decision_ts')}" + (" — BOUND" if dec.get('bound') in (True, 'true') else "")})
    else:
        stages.append({"stage": "Decision", "status": "awaited", "source": "gov_decision_audit",
                       "detail": "No decision logged yet — log one on the Work-a-submission page."})
    return {"found": True, "kind": "submission", "title": id,
            "subtitle": f"{sub.get('cedant_name','')} · {sub.get('structure','')} · {sub.get('zone_name','')}",
            "stages": stages, "agents": agents, "decision": dec}


def _pctf(v):
    try:
        return format(round(float(v), 1), ",") + "%"
    except Exception:
        return "—"


# ─────────────────────────── agents roster ───────────────────────────
@app.get("/api/agents")
def agent_roster():
    # tone = its own colour (solvency-style); link = where the node is actually used in the workbench.
    host = config.workspace_host()
    if host and not host.startswith("http"):
        host = "https://" + host
    genie_link = (f"{host}/genie/rooms/{config.GENIE_SPACE_ID}" if (host and config.GENIE_SPACE_ID) else "")
    nodes = [
        {"kind": "supervisor", "name": "Reinsurance AI supervisor", "tone": "violet", "endpoint": config.resolve_endpoint(config.EP_SUPERVISOR_SUBSTR), "desc": "Composes the specialists into one recommendation. Narrates only — never binds.", "link": "#sub/sub:900002", "link_label": "see it call tools on the portfolio bomb"},
        {"kind": "agent", "name": "Cat-Event Response", "tone": "orange", "endpoint": config.resolve_endpoint(config.EP_EVENT_SUBSTR), "desc": "On a live event, briefs the CRO on book-wide loss, capital hit and the most exposed cedant.", "link": "#event", "link_label": "see it on the Cat Event page"},
        {"kind": "agent", "name": "Portfolio Strategy", "tone": "blue", "endpoint": config.resolve_endpoint(config.EP_PORTFOLIO_SUBSTR), "desc": "Proposes a diversifying alternative when a deal saturates a peak zone.", "link": "#sub/sub:900002", "link_label": "see its alternative on sub:900002"},
        {"kind": "agent", "name": "Counterparty Credit", "tone": "rose", "endpoint": config.resolve_endpoint(config.EP_COUNTERPARTY_SUBSTR), "desc": "Injects the cedant credit + regulatory-watch signal into the decision.", "link": "#sub/sub:900002", "link_label": "see the counterparty panel"},
        {"kind": "agent", "name": "Challenge / Second-Opinion", "tone": "amber", "endpoint": config.resolve_endpoint(config.EP_CHALLENGE_SUBSTR), "desc": "Argues the other side, quantified.", "link": "#sub/sub:900002", "link_label": "see it on sub:900002"},
        {"kind": "agent", "name": "Data Quality", "tone": "emerald", "endpoint": config.resolve_endpoint(config.EP_DATAQUALITY_SUBSTR), "desc": "Bordereaux / exposure completeness narrative.", "link": "#intake", "link_label": "see it on the Ingestion page"},
        {"kind": "tool", "name": "fn_triage_submission", "tone": "blue", "desc": "Appetite decision (model).", "link": "#sub/sub:900002", "link_label": "the Triage card"},
        {"kind": "tool", "name": "fn_price_submission", "tone": "emerald", "desc": "Reinsurance pricing — RoL / burning cost / combined ratio (model).", "link": "#sub/sub:900002", "link_label": "the Price card"},
        {"kind": "tool", "name": "fn_accumulation_impact", "tone": "rose", "desc": "THE CRUX — marginal peak-zone PML vs appetite.", "link": "#sub/sub:900002", "link_label": "the Accumulation card"},
        {"kind": "tool", "name": "fn_capital_impact", "tone": "amber", "desc": "THE CRUX — marginal SCR vs expected return (RoRAC).", "link": "#sub/sub:900002", "link_label": "the Capital card"},
        {"kind": "tool", "name": "fn_event_response", "tone": "orange", "desc": "Book-wide cat-event loss + capital impact in seconds.", "link": "#event", "link_label": "the Cat Event page"},
        {"kind": "genie", "name": "Ask the Portfolio", "tone": "cyan", "desc": "AI/BI Genie over the gold marts — ask the book in natural language.", "space": config.GENIE_SPACE_ID, "link": genie_link, "link_label": "open the Genie space"},
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


@app.get("/api/reset/status")
def reset_status(run_id: int):
    try:
        w = config.get_workspace_client()
        st = w.jobs.get_run(run_id=run_id).state
        lc = getattr(st.life_cycle_state, "value", str(st.life_cycle_state)) if st and st.life_cycle_state else None
        rs = getattr(st.result_state, "value", str(st.result_state)) if st and st.result_state else None
        return {"life_cycle": lc, "result": rs}
    except Exception as e:
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


@app.post("/api/warm-cache")
def warm_cache():
    """Pre-fill the AI cache with exactly what the demo surfaces (same endpoint + question + custom_inputs
    the UI calls, so the keys match → cache hits on stage). Run after a reset, once the data is re-anchored."""
    warmed = []
    for sid in ["sub:900001", "sub:900002"]:
        try:
            r = narrate(sid, role="supervisor")          # the supervisor box on the submission view
            warmed.append({"item": sid, "cache": r.get("cache"), "tools": len(r.get("tools", []))})
        except Exception as e:
            warmed.append({"item": sid, "error": str(e)[:120]})
    try:
        ev = sql.query_one(f"SELECT event_public_id FROM {config.fqn('gold_event_response')} ORDER BY event_date DESC LIMIT 1")
        if ev and ev.get("event_public_id"):
            r = event_narrate(ev["event_public_id"])     # the Cat-Event briefing
            warmed.append({"item": ev["event_public_id"], "cache": r.get("cache")})
    except Exception as e:
        warmed.append({"item": "event", "error": str(e)[:120]})
    return {"status": "warmed", "items": warmed}


# ─────────────────────────── static SPA ───────────────────────────
if os.path.isdir(DIST):
    app.mount("/assets", StaticFiles(directory=os.path.join(DIST, "assets")), name="assets") if os.path.isdir(os.path.join(DIST, "assets")) else None


@app.get("/")
def root():
    return FileResponse(os.path.join(DIST, "index.html"))


@app.get("/healthz")
def healthz():
    return {"ok": True}
