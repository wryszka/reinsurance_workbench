# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Agents — Reinsurance AI (LLM sub-agents + supervisor)
# MAGIC
# MAGIC Deploys real FM-backed serving endpoints. Each is **narrate-only** — it receives the structured findings
# MAGIC the app already computed from the UC functions/marts (passed as `data_json`) and writes prose. No agent
# MAGIC binds; they advise, flag, challenge, escalate. The supervisor composes the sub-agents' findings.
# MAGIC
# MAGIC - `reinsurance-dataquality` — bordereaux / exposure completeness narrative.
# MAGIC - `reinsurance-challenge`   — Challenge / Second-Opinion (argues the other side).
# MAGIC - `reinsurance-supervisor`  — synthesis box (narrates only; never binds).
# MAGIC
# MAGIC The managed Agent Bricks supervisor (Agents UI) is the alternative composition path, captured in
# MAGIC `docs/REINSURANCE_FORUM_DEMO_RUNBOOK.md`. The structured decision panels never parse this prose.

# COMMAND ----------

dbutils.widgets.text("catalog", "lr_dev_aws_us_catalog")
dbutils.widgets.text("schema", "bricksurance_re")
dbutils.widgets.text("fm_endpoint", "databricks-claude-sonnet-4-6")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
FM = dbutils.widgets.get("fm_endpoint")
fqn = f"{catalog}.{schema}"

import mlflow, pandas as pd
from mlflow.models.signature import infer_signature
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ServedEntityInput, EndpointCoreConfigInput

mlflow.set_registry_uri("databricks-uc")

ROLE_SYSTEM = {
    "dataquality": ("You are the Data Quality reviewer on a reinsurance underwriting desk at Bricksurance Re. "
        "Given the structured findings, write 2-3 sentences on bordereaux/exposure completeness, the inbound "
        "channel (clean ADEPT/CDR vs messy manual), and any quarantined rows. Be specific and factual. You flag; you never bind."),
    "challenge": ("You are the Challenge / Second-Opinion underwriter at Bricksurance Re. Argue the OTHER side of "
        "the recommendation: if it looks attractive standalone, surface the portfolio/accumulation/capital risk; "
        "if it looks toxic, note any mitigants. Be sharp and quantified, 3-4 sentences. You challenge and escalate; you never bind."),
    "supervisor": ("You are the Reinsurance AI supervisor at Bricksurance Re. Synthesize the specialists' structured "
        "findings (triage, technical price, marginal accumulation, marginal capital) into ONE orchestrated "
        "recommendation for the underwriter and CRO. Lead with the call (recommend-to-bind / refer / decline), then "
        "the 2-3 quantified reasons. You advise and escalate; humans decide and bind — never state that you have bound."),
}

# COMMAND ----------

class ReinsuranceAgent(mlflow.pyfunc.PythonModel):
    """Narrate-only FM-backed agent. Input columns: role, question, data_json. Returns prose string per row."""
    def load_context(self, context):
        import os
        from mlflow.deployments import get_deploy_client
        self.client = get_deploy_client("databricks")
        self.fm = os.environ.get("FM_ENDPOINT", "databricks-claude-sonnet-4-6")
        self.default_role = os.environ.get("AGENT_ROLE", "supervisor")
        self.systems = {
            "dataquality": "You are the Data Quality reviewer on a reinsurance desk. Given the structured findings, write 2-3 factual sentences on data completeness, inbound channel and quarantined rows. You flag; you never bind.",
            "challenge": "You are the Challenge / Second-Opinion underwriter. Argue the other side, quantified, 3-4 sentences. You challenge and escalate; you never bind.",
            "supervisor": "You are the Reinsurance AI supervisor. Synthesize the structured findings into one orchestrated recommendation: lead with the call (recommend-to-bind / refer / decline) then 2-3 quantified reasons. You advise and escalate; humans bind.",
        }

    def _one(self, role, question, data_json):
        system = self.systems.get(role or self.default_role, self.systems["supervisor"])
        user = f"Question: {question}\n\nStructured findings (already computed by Databricks UC functions — narrate, do not recompute):\n{data_json}"
        try:
            resp = self.client.predict(endpoint=self.fm, inputs={
                "messages": [{"role": "system", "content": system},
                             {"role": "user", "content": user}],
                "max_tokens": 400, "temperature": 0.2})
            return resp["choices"][0]["message"]["content"]
        except Exception as e:
            return f"[narration unavailable: {str(e)[:120]}]"

    def predict(self, context, model_input):
        df = model_input
        out = []
        for _, r in df.iterrows():
            out.append(self._one(r.get("role"), r.get("question", ""), r.get("data_json", "")))
        return out

# COMMAND ----------

example = pd.DataFrame([{"role": "supervisor", "question": "Should we bind sub:900002?",
                         "data_json": '{"triage":"refer","breach_eur":16700000}'}])
sig = infer_signature(example, ["..."])
with mlflow.start_run(run_name="reinsurance_agent"):
    mi = mlflow.pyfunc.log_model(
        artifact_path="model", python_model=ReinsuranceAgent(),
        signature=sig, input_example=example,
        pip_requirements=["mlflow", "pandas"],
        registered_model_name=f"{fqn}.model_reinsurance_agent")
ver = mi.registered_model_version
print("agent model v", ver)

# COMMAND ----------

w = WorkspaceClient()

def deploy_agent(endpoint, role):
    entity = ServedEntityInput(name="agent", entity_name=f"{fqn}.model_reinsurance_agent",
                               entity_version=ver, workload_size="Small", scale_to_zero_enabled=True,
                               environment_vars={"AGENT_ROLE": role, "FM_ENDPOINT": FM})
    existing = [e.name for e in w.serving_endpoints.list()]
    if endpoint in existing:
        w.serving_endpoints.update_config_and_wait(name=endpoint, served_entities=[entity])
    else:
        w.serving_endpoints.create_and_wait(name=endpoint, config=EndpointCoreConfigInput(name=endpoint, served_entities=[entity]))
    print("deployed", endpoint, "(role:", role + ")")

deploy_agent("reinsurance-dataquality", "dataquality")
deploy_agent("reinsurance-challenge", "challenge")
deploy_agent("reinsurance-supervisor", "supervisor")
print("agents ready")
