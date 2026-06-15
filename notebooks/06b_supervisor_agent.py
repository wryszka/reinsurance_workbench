# Databricks notebook source
# MAGIC %md
# MAGIC # 06b · Reinsurance AI supervisor — a REAL tool-calling agent (Mosaic AI Agent Framework)
# MAGIC
# MAGIC A `ChatAgent` with a genuine Claude tool-use loop: the LLM **autonomously decides which UC functions to
# MAGIC call** (triage, price, accumulation, capital, recommendation, event response, portfolio alternative,
# MAGIC counterparty, Genie), executes them via the SQL Statement Execution API, reasons over the results, and
# MAGIC returns a grounded answer + a tool-call trace. Deployed via `databricks.agents.deploy` as a Model Serving
# MAGIC endpoint with the UC functions declared as `DatabricksFunction` resources (automatic authorization).
# MAGIC
# MAGIC This is the AI pillar made real — the agent calls the tools; it does not narrate pre-computed data.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
dbutils.widgets.text("fm_endpoint", "databricks-claude-sonnet-4-6")
dbutils.widgets.text("warehouse_id", "a3b61648ea4809e3")
dbutils.widgets.text("genie_space_id", "")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")
fm_endpoint = dbutils.widgets.get("fm_endpoint"); warehouse_id = dbutils.widgets.get("warehouse_id")
genie_space_id = dbutils.widgets.get("genie_space_id")
fqn = f"{catalog}.{schema}"

import json, uuid, os
import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentResponse
mlflow.set_registry_uri("databricks-uc")

SYSTEM = (
    "You are the Reinsurance AI supervisor at Bricksurance Re, a treaty reinsurer. You help an underwriter or "
    "the CRO decide on submissions and respond to catastrophe events. You have tools that call real Unity Catalog "
    "functions — USE THEM to ground every claim in numbers; never invent figures. For a submission, typically call "
    "get_submission_summary, get_triage, get_price, get_accumulation, get_capital and get_recommendation, then give "
    "the call (recommend-to-bind / refer / decline) with 2-3 quantified reasons. If a deal is referred on "
    "accumulation, also call get_portfolio_alternative and get_counterparty. For an event, call get_event_response "
    "(and get_event_treaties for detail). You advise and escalate; humans bind — never say you have bound a deal.")

# Tool schemas advertised to Claude (the LLM chooses which to call).
TOOLS = {
 "get_submission_summary": ("Factual summary of a submission: cedant, broker, structure, LoB, territories, perils, peak zone, layer, subject premium, RoL, cedant rating, data quality, inbound channel.", "submission_public_id"),
 "get_triage": ("Appetite triage for a submission: fast_track / refer / decline, confidence, reasons.", "submission_public_id"),
 "get_price": ("Reinsurance technical price for a submission (RoL / burning cost / combined ratio): predicted loss ratio, combined ratio, offered RoL, rate adequacy, verdict.", "submission_public_id"),
 "get_accumulation": ("Marginal peak-zone 1-in-200 PML impact of a submission vs CRO appetite: current/marginal/after PML, breach amount, correlated in-force treaties.", "submission_public_id"),
 "get_capital": ("Marginal capital for a submission: expected return, marginal SCR, RoRAC vs the 15% hurdle, capital_destructive flag.", "submission_public_id"),
 "get_recommendation": ("The orchestrated bind/refer/decline recommendation for a submission with its basis.", "submission_public_id"),
 "get_portfolio_alternative": ("A diversifying alternative deal away from a saturated peak zone (zone with headroom, suggested capacity, est RoRAC).", "submission_public_id"),
 "get_counterparty": ("Cedant credit quality + any regulatory-watch signal for a submission's cedant.", "submission_public_id"),
 "get_portfolio_position": ("Peak-zone capacity vs appetite (pass a zone_id like EU_WIND, or 'ALL' for the worst-utilised zone).", "zone_id"),
 "get_event_response": ("Book-wide response to a catastrophe event: treaties responding, gross/net loss, reinstatement, Solvency II ratio before/after, top exposed cedant.", "event_public_id"),
 "get_event_treaties": ("The individual in-force treaties that respond to a catastrophe event, with ceded loss and reinstatement.", "event_public_id"),
 "ask_the_portfolio": ("Ask a natural-language analytics question over the portfolio marts via AI/BI Genie.", "question"),
}

# COMMAND ----------

def _run_sql(sql):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
    w = WorkspaceClient()
    wid = os.environ.get("AGENT_WAREHOUSE_ID", warehouse_id)
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=wid, wait_timeout="50s")
    if r.status and r.status.state == StatementState.FAILED:
        raise RuntimeError(r.status.error.message if r.status.error else "SQL failed")
    if not (r.manifest and r.manifest.schema and r.manifest.schema.columns):
        return []
    cols = [c.name for c in r.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in (r.result.data_array or [])] if r.result else []


def _genie_ask(space_id, question):
    from databricks.sdk import WorkspaceClient
    if not space_id or not question:
        return {"note": "Genie space not configured"}
    try:
        w = WorkspaceClient()
        m = w.genie.start_conversation_and_wait(space_id=space_id, content=question)
        out = {"answer": None, "query": None}
        for att in (m.attachments or []):
            if att.text and att.text.content: out["answer"] = att.text.content[:1500]
            if att.query and att.query.query: out["query"] = att.query.query[:600]
        return out
    except Exception as e:
        return {"error": f"genie unavailable: {e}"}


def _call_fm(endpoint, messages, tools):
    import requests
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    host = w.config.host.rstrip("/"); hdr = w.config._header_factory()
    r = requests.post(f"{host}/serving-endpoints/{endpoint}/invocations",
                      headers={**hdr, "Content-Type": "application/json"},
                      json={"messages": messages, "tools": tools, "tool_choice": "auto",
                            "max_tokens": 1200, "temperature": 0.1}, timeout=120)
    r.raise_for_status()
    return r.json()


class ReinsuranceSupervisor(ChatAgent):
    def __init__(self, catalog, schema, fm_endpoint, genie_space_id):
        self.fqn = f"{catalog}.{schema}"; self.fm = fm_endpoint; self.genie = genie_space_id

    def _scalar(self, fn, arg):
        rows = _run_sql(f"SELECT to_json(`{self.fqn.split('.')[0]}`.`{self.fqn.split('.')[1]}`.{fn}('{arg}')) AS r")
        return json.loads(rows[0]["r"]) if rows and rows[0].get("r") else {"error": "no row"}

    def _tool(self, name, args):
        a = args or {}
        sid = a.get("submission_public_id", ""); eid = a.get("event_public_id", "")
        if name == "get_submission_summary":   return self._scalar("fn_submission_summary", sid)
        if name == "get_triage":               return self._scalar("fn_triage_submission", sid)
        if name == "get_price":                return self._scalar("fn_price_submission", sid)
        if name == "get_accumulation":         return self._scalar("fn_accumulation_impact", sid)
        if name == "get_capital":              return self._scalar("fn_capital_impact", sid)
        if name == "get_recommendation":       return self._scalar("fn_recommendation", sid)
        if name == "get_portfolio_alternative":return self._scalar("fn_portfolio_alternative", sid)
        if name == "get_portfolio_position":   return self._scalar("fn_portfolio_position", a.get("zone_id", "ALL"))
        if name == "get_event_response":       return self._scalar("fn_event_response", eid)
        if name == "get_event_treaties":       return _run_sql(f"SELECT * FROM {self.fqn}.fn_event_treaty_detail('{eid}') LIMIT 15")
        if name == "get_counterparty":         return _run_sql(f"SELECT c.cedant_name, c.rating, c.credit_quality_step, c.outlook, c.regulatory_watch, c.watch_note FROM {self.fqn}.silver_submissions s JOIN {self.fqn}.counterparties c ON s.cedant_id=c.cedant_id WHERE s.submission_public_id='{sid}' LIMIT 1")
        if name == "ask_the_portfolio":        return _genie_ask(self.genie, a.get("question", ""))
        return {"error": f"unknown tool {name}"}

    def predict(self, messages, context=None, custom_inputs=None) -> ChatAgentResponse:
        ci = custom_inputs or {}
        hint = ""
        if ci.get("submission_public_id"): hint += f"\nThe submission under review is submission_public_id='{ci['submission_public_id']}'. Pass this id to the submission tools."
        if ci.get("event_public_id"): hint += f"\nThe event under review is event_public_id='{ci['event_public_id']}'. Pass this id to the event tools."
        full = [{"role": "system", "content": SYSTEM + hint}]
        for m in messages:
            full.append({"role": m.role, "content": m.content or ""})
        tools = [{"type": "function", "function": {"name": n,
                  "description": TOOLS[n][0],
                  "parameters": {"type": "object", "properties": {TOOLS[n][1]: {"type": "string"}},
                                 "required": [TOOLS[n][1]]}}} for n in TOOLS]
        trace, final = [], ""
        for _hop in range(8):
            resp = _call_fm(self.fm, full, tools)
            choices = resp.get("choices") or []
            if not choices: break
            msg = choices[0].get("message") or {}
            tcs = msg.get("tool_calls") or []
            if tcs:
                full.append({"role": "assistant", "content": msg.get("content") or "", "tool_calls": tcs})
                for tc in tcs:
                    fnm = (tc.get("function") or {}).get("name")
                    raw = (tc.get("function") or {}).get("arguments") or "{}"
                    try: a = json.loads(raw) if isinstance(raw, str) else (raw or {})
                    except Exception: a = {}
                    res = self._tool(fnm, a)
                    trace.append({"tool": fnm, "args": a})
                    full.append({"role": "tool", "tool_call_id": tc.get("id") or fnm,
                                 "content": json.dumps(res, default=str)[:8000]})
                continue
            final = msg.get("content") or ""
            break
        return ChatAgentResponse(
            messages=[ChatAgentMessage(role="assistant", content=final, id=str(uuid.uuid4()))],
            custom_outputs={"trace": trace, "model": self.fm})

# COMMAND ----------

# Local smoke before logging — proves the tool loop calls real UC functions.
_local = ReinsuranceSupervisor(catalog, schema, fm_endpoint, genie_space_id)
_r = _local.predict([ChatAgentMessage(role="user", content="Should we bind this? Give the call.", id="u1")],
                    custom_inputs={"submission_public_id": "sub:900002"})
print("LOCAL:", _r.messages[0].content[:600])
print("TOOLS CALLED:", [t["tool"] for t in (_r.custom_outputs or {}).get("trace", [])])

# COMMAND ----------

# MAGIC %md ## Log + register + deploy

# COMMAND ----------

from mlflow.models.resources import (DatabricksServingEndpoint, DatabricksFunction,
                                      DatabricksTable, DatabricksGenieSpace, DatabricksSQLWarehouse)
FNS = ["fn_submission_summary", "fn_triage_submission", "fn_price_submission", "fn_accumulation_impact",
       "fn_capital_impact", "fn_recommendation", "fn_portfolio_alternative", "fn_portfolio_position",
       "fn_event_response", "fn_event_treaty_detail"]
resources = [DatabricksServingEndpoint(endpoint_name=fm_endpoint), DatabricksSQLWarehouse(warehouse_id=warehouse_id)]
resources += [DatabricksFunction(function_name=f"{fqn}.{fn}") for fn in FNS]
for t in ["silver_submissions", "counterparties", "inforce_accumulation", "gold_portfolio_position",
          "gold_event_response", "event_treaty_losses", "feature_submission", "ref_cedants"]:
    resources.append(DatabricksTable(table_name=f"{fqn}.{t}"))
if genie_space_id:
    resources.append(DatabricksGenieSpace(genie_space_id=genie_space_id))

agent_uc_name = f"{fqn}.reinsurance_agent"
input_example = {"messages": [{"role": "user", "content": "Should we bind this? Give the call."}],
                 "custom_inputs": {"submission_public_id": "sub:900002"}}
with mlflow.start_run(run_name="reinsurance_supervisor_agent"):
    mi = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=ReinsuranceSupervisor(catalog, schema, fm_endpoint, genie_space_id),
        resources=resources, input_example=input_example,
        registered_model_name=agent_uc_name,
        pip_requirements=["mlflow", "databricks-sdk>=0.30.0", "requests"])
    print("logged:", mi.model_uri)

from mlflow.tracking import MlflowClient
mc = MlflowClient(registry_uri="databricks-uc")
version = max(int(v.version) for v in mc.search_model_versions(f"name='{agent_uc_name}'"))

from databricks import agents
dep = agents.deploy(model_name=agent_uc_name, model_version=version, scale_to_zero=True,
                    environment_vars={"AGENT_WAREHOUSE_ID": warehouse_id},
                    tags={"project": "reinsurance_workbench", "layer": "agent"})
ep_name = getattr(dep, "endpoint_name", None) or getattr(dep, "endpoint", None)
print("agents.deploy ->", ep_name)
dbutils.notebook.exit(json.dumps({"endpoint_name": str(ep_name), "version": version}))
