"""MCP server — the Reinsurance Workbench exposed as callable tools.

MCP-first: the app's own endpoint functions are the single implementation; this
exposes them as an MCP tool surface so the app UI, notebooks and external agents
— the Bricksurance control tower included — are all clients of one surface.

Every tool DELEGATES to the app endpoint function (passed in via register), so it
reuses the exact logic AND any server-side gate it enforces. Reads are idempotent;
[action] tools write through the governed handler. A 401/403 becomes a clean
{"ok": False, "gated": True}.

Transport: JSON-RPC 2.0 over one POST + a GET manifest — mirrors
pricing-workbench-gen2 / reserving-workbench / ifrs17-workbench.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/mcp", tags=["mcp"])

PROTOCOL_VERSION = "2025-06-18"
SERVER_INFO = {"name": "bricksurance-reinsurance-workbench", "version": "1.0.0"}


def _mk(name, desc, props=None, required=None):
    return {"name": name, "description": desc,
            "inputSchema": {"type": "object", "properties": props or {}, "required": required or []}}


def _wrap(fn, *args, **kwargs) -> dict:
    try:
        r = fn(*args, **kwargs)
    except HTTPException as e:
        gated = e.status_code in (401, 403)
        return {"ok": False, **({"gated": True} if gated else {}), "error": f"{e.status_code}: {e.detail}"}
    except Exception as e:
        logger.warning("mcp reinsurance delegate %s failed: %s", getattr(fn, "__name__", "?"), str(e)[:200])
        return {"ok": False, "error": str(e)[:200]}
    return r if isinstance(r, dict) else {"ok": True, "data": r}


_SID = {"sid": {"type": "string", "description": "Submission public id"}}
_EID = {"eid": {"type": "string", "description": "Cat-event id"}}


# --- control tower ---
def _t_ct_overview(a, app):   return _wrap(app.control_tower)
def _t_ct_zone(a, app):       return _wrap(app.control_tower_zone, str(a.get("zone_id") or ""))
def _t_ct_capital(a, app):    return _wrap(app.control_tower_capital)
# --- submissions ---
def _t_sub_list(a, app):        return _wrap(app.submissions)
def _t_sub_decision(a, app):    return _wrap(app.decision, str(a.get("sid") or ""))
def _t_sub_alternative(a, app): return _wrap(app.alternative, str(a.get("sid") or ""))
def _t_sub_counterparty(a, app):return _wrap(app.counterparty, str(a.get("sid") or ""))
def _t_sub_whatif(a, app):      return _wrap(app.whatif, str(a.get("sid") or ""), float(a.get("limit_eur") or 30000000), float(a.get("attachment_eur") or 20000000))
def _t_sub_narrate(a, app):     return _wrap(app.narrate, str(a.get("sid") or ""), str(a.get("role") or "supervisor"))
# --- cat events ---
def _t_event_list(a, app):      return _wrap(app.events)
def _t_event_response(a, app):  return _wrap(app.event_response, str(a.get("eid") or ""))
def _t_event_treaties(a, app):  return _wrap(app.event_treaties, str(a.get("eid") or ""))
def _t_event_narrate(a, app):   return _wrap(app.event_narrate, str(a.get("eid") or ""))
def _t_intake(a, app):          return _wrap(app.intake)
# --- ingestion ---
def _t_ing_feeds(a, app):       return _wrap(app.ingestion_feeds)
def _t_ing_scorecard(a, app):   return _wrap(app.ingestion_scorecard)
def _t_ing_quarantine(a, app):  return _wrap(app.ingestion_quarantine)
def _t_ing_document(a, app):    return _wrap(app.ingestion_document)
def _t_ing_geo(a, app):         return _wrap(app.ingestion_geo)
def _t_ing_sources(a, app):     return _wrap(app.ingestion_sources)
def _t_ing_storm(a, app):       return _wrap(app.ingestion_storm)
# --- accumulation (YELT) ---
def _t_yelt_summary(a, app):    return _wrap(app.yelt_summary)
def _t_yelt_ep(a, app):         return _wrap(app.yelt_ep)
# --- governance ---
def _t_gov_inventory(a, app):   return _wrap(app.gov_inventory)
def _t_gov_audit(a, app):       return _wrap(app.gov_audit, str(a.get("sid") or ""))
def _t_gov_audit_all(a, app):   return _wrap(app.gov_audit_all)
def _t_gov_lineage(a, app):     return _wrap(app.gov_lineage)
def _t_gov_masking(a, app):     return _wrap(app.gov_masking)
def _t_gov_models(a, app):      return _wrap(app.gov_models)
def _t_gov_ai_activity(a, app): return _wrap(app.gov_ai_activity_route, str(a.get("id") or ""))
def _t_gov_track(a, app):       return _wrap(app.gov_track, str(a.get("id") or ""))
# --- AI ---
def _t_ai_agents(a, app):       return _wrap(app.agent_roster)
def _t_ai_ask(a, app):          return _wrap(app.agent_ask, str(a.get("q") or ""), str(a.get("sid") or ""), str(a.get("eid") or ""))
def _t_genie_examples(a, app):  return _wrap(app.genie_examples)
def _t_genie_ask(a, app):       return _wrap(app.genie_ask, str(a.get("q") or ""))
# --- governed action ---
def _t_log_decision(a, app):    return _wrap(app.log_decision, {"submission_public_id": a.get("submission_public_id") or a.get("sid"), "action": a.get("action"), "decided_by": a.get("decided_by")})


TOOL_SCHEMAS: list[dict[str, Any]] = [
    _mk("ct_overview", "The reinsurance control tower — portfolio view across peril zones, capacity and capital headroom."),
    _mk("ct_zone", "Control-tower detail for one accumulation/peril zone.", {"zone_id": {"type": "string"}}, ["zone_id"]),
    _mk("ct_capital", "Capital headroom / Solvency II impact view for the treaty portfolio."),
    _mk("sub_list", "List treaty submissions in the pipeline."),
    _mk("sub_decision", "The pricing + accept/decline decision for a submission (rate-on-line, marginal accumulation).", _SID, ["sid"]),
    _mk("sub_alternative", "Alternative structures offered for a submission.", _SID, ["sid"]),
    _mk("sub_counterparty", "Counterparty / cedant view for a submission.", _SID, ["sid"]),
    _mk("sub_whatif", "Re-price a submission under a different limit / attachment and see the marginal accumulation + capital impact.", {**_SID, "limit_eur": {"type": "number"}, "attachment_eur": {"type": "number"}}, ["sid"]),
    _mk("sub_narrate", "Narrate a submission decision in plain language for a given role.", {**_SID, "role": {"type": "string"}}, ["sid"]),
    _mk("event_list", "Live cat events being tracked."),
    _mk("event_response", "The response view for a cat event (exposed treaties, expected loss).", _EID, ["eid"]),
    _mk("event_treaties", "Treaties exposed to a cat event.", _EID, ["eid"]),
    _mk("event_narrate", "Narrate a cat-event response in plain language.", _EID, ["eid"]),
    _mk("intake", "The submission-intake view (what's arriving)."),
    _mk("ingest_feeds", "Source feeds feeding the reinsurance workbench + freshness."),
    _mk("ingest_scorecard", "Ingestion data-quality scorecard."),
    _mk("ingest_quarantine", "Quarantined inbound records."),
    _mk("ingest_document", "Ingested submission documents (slips, SOVs)."),
    _mk("ingest_geo", "Geospatial exposure ingestion view."),
    _mk("ingest_sources", "The labelled ingestion sources."),
    _mk("ingest_storm", "Storm / event-set ingestion view."),
    _mk("yelt_summary", "Year-event-loss-table (YELT) accumulation summary for the portfolio."),
    _mk("yelt_ep", "Exceedance-probability (EP) curve from the YELT."),
    _mk("gov_inventory", "Governed-asset inventory for reinsurance."),
    _mk("gov_audit", "Audit trail for one submission.", _SID, ["sid"]),
    _mk("gov_audit_all", "The full governance audit trail."),
    _mk("gov_lineage", "Lineage view (submission → decision → capital)."),
    _mk("gov_masking", "PII masking / access-control view."),
    _mk("gov_models", "Registered models under governance."),
    _mk("gov_ai_activity", "AI-activity log (optionally for one entity).", {"id": {"type": "string"}}),
    _mk("gov_track", "Track every AI touch on an entity.", {"id": {"type": "string"}}, ["id"]),
    _mk("ai_agents", "The AI agent roster in the workbench."),
    _mk("ai_ask", "Ask the grounded reinsurance assistant a question (optionally about a submission / event).", {"q": {"type": "string"}, "sid": {"type": "string"}, "eid": {"type": "string"}}, ["q"]),
    _mk("genie_examples", "Example Genie data questions."),
    _mk("genie_ask", "Ask AI/BI Genie a data question over the reinsurance space.", {"q": {"type": "string"}}, ["q"]),
    _mk("act_log_decision", "[action] Log an underwriting decision on a submission (accept/decline) — audited.", {"submission_public_id": {"type": "string"}, "action": {"type": "string"}, "decided_by": {"type": "string"}}, ["submission_public_id", "action"]),
]

TOOL_IMPLS = {
    "ct_overview": _t_ct_overview, "ct_zone": _t_ct_zone, "ct_capital": _t_ct_capital,
    "sub_list": _t_sub_list, "sub_decision": _t_sub_decision, "sub_alternative": _t_sub_alternative,
    "sub_counterparty": _t_sub_counterparty, "sub_whatif": _t_sub_whatif, "sub_narrate": _t_sub_narrate,
    "event_list": _t_event_list, "event_response": _t_event_response, "event_treaties": _t_event_treaties,
    "event_narrate": _t_event_narrate, "intake": _t_intake,
    "ingest_feeds": _t_ing_feeds, "ingest_scorecard": _t_ing_scorecard, "ingest_quarantine": _t_ing_quarantine,
    "ingest_document": _t_ing_document, "ingest_geo": _t_ing_geo, "ingest_sources": _t_ing_sources, "ingest_storm": _t_ing_storm,
    "yelt_summary": _t_yelt_summary, "yelt_ep": _t_yelt_ep,
    "gov_inventory": _t_gov_inventory, "gov_audit": _t_gov_audit, "gov_audit_all": _t_gov_audit_all,
    "gov_lineage": _t_gov_lineage, "gov_masking": _t_gov_masking, "gov_models": _t_gov_models,
    "gov_ai_activity": _t_gov_ai_activity, "gov_track": _t_gov_track,
    "ai_agents": _t_ai_agents, "ai_ask": _t_ai_ask, "genie_examples": _t_genie_examples, "genie_ask": _t_genie_ask,
    "act_log_decision": _t_log_decision,
}


def _ok(rpc_id, result):  return {"jsonrpc": "2.0", "id": rpc_id, "result": result}
def _err(rpc_id, code, m): return {"jsonrpc": "2.0", "id": rpc_id, "error": {"code": code, "message": m}}


def register(app_module):
    @router.post("")
    async def jsonrpc(request: Request):
        try:
            body = await request.json()
        except Exception:
            return _err(None, -32700, "Parse error: body is not valid JSON")
        rpc_id = body.get("id"); method = body.get("method"); params = body.get("params") or {}
        if method == "initialize":
            return _ok(rpc_id, {
                "protocolVersion": PROTOCOL_VERSION, "serverInfo": SERVER_INFO,
                "capabilities": {"tools": {}},
                "instructions": ("Reinsurance treaty-submission workbench for Bricksurance Re. Reads cover the "
                                 "control tower, submissions, cat events, ingestion, YELT accumulation and governance. "
                                 "The one action (log a decision) writes through the same governed handler as the UI. "
                                 "Never invent a figure.")})
        if method in ("notifications/initialized", "notifications/cancelled"):
            return _ok(rpc_id, {})
        if method == "tools/list":
            return _ok(rpc_id, {"tools": TOOL_SCHEMAS})
        if method == "tools/call":
            name = params.get("name"); args = params.get("arguments") or {}
            impl = TOOL_IMPLS.get(name)
            if impl is None:
                return _err(rpc_id, -32601, f"Unknown tool: {name}")
            try:
                payload = impl(args, app_module)
            except Exception as e:
                logger.exception("mcp tool %s failed", name)
                return _err(rpc_id, -32603, f"Tool execution failed: {str(e)[:200]}")
            return _ok(rpc_id, {
                "content": [{"type": "text", "text": json.dumps(payload, default=str)}],
                "structuredContent": payload,
                "isError": isinstance(payload, dict) and payload.get("ok") is False})
        return _err(rpc_id, -32601, f"Method not found: {method}")

    @router.get("/manifest")
    async def manifest():
        return {"server": SERVER_INFO, "protocol_version": PROTOCOL_VERSION,
                "tools": [{"name": t["name"], "description": t["description"]} for t in TOOL_SCHEMAS]}

    return router
