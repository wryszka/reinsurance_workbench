"""Bricksurance Re — Reinsurance Intelligence Workbench (thin app).

Presentation only. Every panel calls a real Databricks object (UC function / gold mart / serving endpoint /
Genie) and renders. No business logic, no scoring, no transformation here. The supervisor narration box calls
the supervisor endpoint through the USE_CACHE wrapper (narration only); structured panels never parse prose.
"""
import json, os
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
    return {"zones": pos, "capital": cap[0] if cap else {}, "cat_curves": cat}


# ─────────────────────────── underwriter queue ───────────────────────────
@app.get("/api/submissions")
def submissions():
    return {"rows": sql.query(f"""
        SELECT s.submission_public_id, s.cedant_name, s.broker, s.structure, s.lob, s.zone_name,
               s.proportional_or_xol, s.rol_pct, s.rating, s.data_quality_score, s.inbound_channel,
               s.is_cat_xol, s.is_peak
        FROM {config.fqn('silver_submissions')} s ORDER BY s.submission_public_id""")}


# ─────────────────────────── hero decision view (structured = real UC fns) ───────────────────────────
@app.get("/api/submission/{sid}/decision")
def decision(sid: str):
    s = sql.esc(sid)
    summary = _struct(f"{config.fqn('fn_submission_summary')}('{s}')")
    triage = _struct(f"{config.fqn('fn_triage_submission')}('{s}')")
    price = _struct(f"{config.fqn('fn_price_submission')}('{s}')")
    accumulation = _struct(f"{config.fqn('fn_accumulation_impact')}('{s}')")
    capital = _struct(f"{config.fqn('fn_capital_impact')}('{s}')")
    # recommendation is derived from the structured outputs (not an LLM)
    if triage.get("decision") == "decline" or price.get("verdict") == "inadequate":
        rec = "decline"
    elif accumulation.get("breaches_appetite") or capital.get("capital_destructive") or triage.get("decision") == "refer":
        rec = "refer"
    else:
        rec = "recommend-to-bind"
    return {"summary": summary, "triage": triage, "price": price,
            "accumulation": accumulation, "capital": capital, "recommendation": rec}


@app.get("/api/submission/{sid}/narrate")
def narrate(sid: str, role: str = "supervisor"):
    d = decision(sid)
    role_substr = {"supervisor": config.EP_SUPERVISOR_SUBSTR, "challenge": config.EP_CHALLENGE_SUBSTR,
                   "dataquality": config.EP_DATAQUALITY_SUBSTR}.get(role, config.EP_SUPERVISOR_SUBSTR)
    q = {"supervisor": f"Give the orchestrated recommendation for {sid}.",
         "challenge": f"Argue the other side on {sid}.",
         "dataquality": f"Assess the data quality for {sid}."}.get(role)
    out = agents.narrate(role_substr, q, d)
    return out


# ─────────────────────────── intake (ADEPT/CDR vs manual + quarantine) ───────────────────────────
@app.get("/api/intake")
def intake():
    channels = sql.query(f"""SELECT path, completeness_band, count(*) AS n
                             FROM {config.fqn('bronze_inbound_audit')} GROUP BY path, completeness_band ORDER BY path""")
    quarantine = sql.query(f"""SELECT submission_public_id, peril, quarantine_reason
                              FROM {config.fqn('bronze_quarantine_loss')}""")
    return {"channels": channels, "quarantine": quarantine}


# ─────────────────────────── governance ───────────────────────────
@app.get("/api/governance/inventory")
def gov_inventory():
    return {"inventory": sql.query(f"SELECT * FROM {config.fqn('gov_data_inventory')} ORDER BY sensitivity_tier"),
            "counterparties": sql.query(f"SELECT * FROM {config.fqn('gov_counterparty_checks')} ORDER BY cedant_id"),
            "solvency": sql.query(f"SELECT DISTINCT solvency_ratio_pct, diversified_bscr_eur, eligible_own_funds_eur, note FROM {config.fqn('gov_solvency_crosslink')}")}


@app.get("/api/governance/audit/{sid}")
def gov_audit(sid: str):
    return {"audit": sql.query(f"SELECT * FROM {config.fqn('fn_decision_audit')}('{sql.esc(sid)}')")}


# ─────────────────────────── agents roster ───────────────────────────
@app.get("/api/agents")
def agent_roster():
    nodes = [
        {"kind": "supervisor", "name": "Reinsurance AI supervisor", "endpoint": config.resolve_endpoint(config.EP_SUPERVISOR_SUBSTR), "desc": "Synthesises specialists into one recommendation. Narrates only — never binds."},
        {"kind": "agent", "name": "Challenge / Second-Opinion", "endpoint": config.resolve_endpoint(config.EP_CHALLENGE_SUBSTR), "desc": "Argues the other side, quantified."},
        {"kind": "agent", "name": "Data Quality", "endpoint": config.resolve_endpoint(config.EP_DATAQUALITY_SUBSTR), "desc": "Bordereaux / exposure completeness narrative."},
        {"kind": "tool", "name": "fn_triage_submission", "desc": "Appetite decision (model)."},
        {"kind": "tool", "name": "fn_price_submission", "desc": "Technical price / rate adequacy (model)."},
        {"kind": "tool", "name": "fn_accumulation_impact", "desc": "THE CRUX — marginal peak-zone PML vs appetite."},
        {"kind": "tool", "name": "fn_capital_impact", "desc": "THE CRUX — marginal SCR vs expected return (RoRAC)."},
        {"kind": "tool", "name": "fn_portfolio_position", "desc": "Peak-zone capacity vs appetite."},
        {"kind": "genie", "name": "Ask the Portfolio", "desc": "AI/BI Genie over the gold marts.", "space": config.GENIE_SPACE_ID},
    ]
    return {"nodes": nodes}


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
