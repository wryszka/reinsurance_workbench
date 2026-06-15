# REDEPLOYABILITY_AUDIT.md (P9)

Mirrors the solvency P6a pattern. The build is redeployable to any serverless workspace by overriding one
variable (`catalog`). Schema is fixed `bricksurance_re`.

## B1 — Hardcoded references
- **Catalog:** variable `catalog` (default `lr_dev_aws_us_catalog`) — flows via DAB → job/pipeline params →
  `dbutils.widgets` / pipeline `configuration` → app env. **No hardcoded catalog in notebooks.**
- **Warehouse:** `warehouse_id` variable; app reads `WAREHOUSE_ID` (valueFrom `sql_warehouse` resource).
- **Workspace host/profile:** only in `databricks.yml` target (dev). No workspace IDs baked elsewhere.
- **Endpoint names:** resolved by **substring** at runtime (`config.resolve_endpoint`, UC-fn creation) — dev-prefix safe.
- **Illustrative constants** (capital factors, crux parameters, own funds): centralised at the top of
  `03_gold_dlt.py` and `05b_crux.py`; flagged in the app's About box. Not workspace-specific.

## B2 — Out-of-bundle workspace state (created by jobs / imperatively)
| Object | Created by | In bundle? |
|---|---|---|
| Schema + landing volume | `00_setup` | no (needs catalog perms) |
| Bronze/silver/gold tables | pipeline `reinsurance_medallion` | pipeline yes; tables no (DLT-managed) |
| `feature_submission` (Feature Store) | `04_features` | no |
| `model_triage_classifier`, `model_loss_ratio` | `05_models` | no (MLflow UC) |
| Serving endpoints `reinsurance-{triage,pricing}` | `05_models` (imperative) | declared in app.yml for grants |
| UC functions `fn_*` | `05b_crux`, `06_agent_tools`, `07_governance` | no |
| Agent endpoints `reinsurance-{supervisor,challenge,dataquality}` | `06_agents` (imperative) | declared in app.yml |
| Governance tables + audit | `07_governance` | no |
| Genie space "Ask the Portfolio" | manual (set `GENIE_SPACE_ID`) | no |
| App `reinsurance-workbench` | bundle | yes |

## B3 — Manual setup steps (ordered, after `bundle deploy -t dev`)
1. `databricks bundle run reinsurance_00_setup -t dev`
2. `databricks bundle run reinsurance_medallion -t dev`   (DLT)
3. `databricks bundle run reinsurance_05_ml -t dev`        (features + models + crux; serving deploy ~10 min)
4. `databricks bundle run reinsurance_06_ai -t dev`        (agent tools + agents + governance)
4b. **GOTCHA:** app.yml serving-endpoint resource bindings only grant CAN_QUERY if the endpoint exists *and is
   resolvable* at app-deploy time — endpoints still mid-build (NOT_READY) are skipped, so the app gets
   "You do not have permission to query the endpoint". Fix: after the agent endpoints are READY, grant CAN_QUERY
   to the app SP explicitly via `PATCH /api/2.0/permissions/serving-endpoints/{id}` (done for event/portfolio/counterparty).
5. Grant the **app service principal**: `CAN_USE` warehouse; `CAN_QUERY` the agent + model endpoints (handled by app.yml
   resource bindings); `USE CATALOG` + `USE SCHEMA` + `SELECT` + `EXECUTE` + `MODIFY` + **`CREATE TABLE`** on
   `bricksurance_re` (CREATE TABLE is required so the app can create the narration cache table on first run);
   `CAN_MANAGE_RUN` on `reinsurance_99_reset`.
6. Set `app_service_principal_id` var + (optionally) declare the reset-job CAN_MANAGE_RUN permission, redeploy.
7. (Optional) Create the Genie space; set `GENIE_SPACE_ID` in app.yaml.
8. Pre-warm: open both heroes, or run `reinsurance_98_smoke_test`.

## B4 — External dependencies
Foundation Model API (`databricks-claude-sonnet-4-6`), MLflow Model Registry + Serving, UC, SQL warehouse,
Databricks Apps, AI/BI Genie (optional). No third-party network calls. Cat-vendor output is synthetic (no vendor API).

## B5 — Idempotency
- Generator + DLT are **full-refresh / overwrite** → re-runnable, deterministic (seed=42).
- `CREATE OR REPLACE FUNCTION` is idempotent — **but revokes EXECUTE grants** to any agent SP; re-grant or
  re-run `06_agents` / app-SP grants after recreating functions.
- Serving deploy uses `update_config_and_wait` if the endpoint exists, else create — idempotent.
- Feature table uses `create_table` first run, `write_table(mode="merge")` after — idempotent.

## B6 — Verification gaps closed by `98_smoke_test`
Data freshness, quarantine, hero silver/feature presence, gold RAG + solvency sanity, triage/price/accumulation/
capital hero assertions, governance audit, serving + agent endpoint existence. **Not yet checked:** live narration
round-trip (cold-start latency), Genie validity, app health beyond `/healthz`, cross-workspace deploy (dry-run only).
