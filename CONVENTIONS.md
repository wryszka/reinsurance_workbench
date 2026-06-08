# CONVENTIONS.md — Reinsurance Workbench

Mirrors `claims_workbench`, `pricing-workbench`, `solvency-ii-qrt-demo-pnc-agentic`, and the `insurance_mrc_poc` extraction pattern (all in `~/vibe`). This is the single source of conventions; every phase follows it. **Mirror, don't invent.**

## Deploy target
- **Workspace (dev):** `fevm-lr-dev-aws-us`, CLI profile `DEV`.
- **Catalog:** variable `catalog` (default `lr_dev_aws_us_catalog`). Portable — change one variable.
- **Schema (fixed):** `bricksurance_re`.
- **Warehouse:** `a3b61648ea4809e3` (Serverless Starter).
- All compute serverless: DLT `serverless: true`; jobs use `environment_key` + `client: "5"`, no clusters.

## DAB layout (mirror claims_workbench + pricing)
```
databricks.yml            # bundle + variables + targets (dev default) + include resources/*.yml
resources/*.yml           # one file per pipeline / job / serving endpoint / app
notebooks/                # numbered: 00_setup_and_data_generation, 01_bronze_dlt, 02_silver_dlt,
                          #   03_gold, 04_features, 05_models, 05b_crux, 06_agent_tools, 06_agents,
                          #   07_governance, 09_reset, 98_smoke_test
app/                      # thin FastAPI + self-contained dist/index.html (no npm egress here)
docs/                     # PROVENANCE_MAPPING.md, REINSURANCE_101_COURSE.md, runbook, talk track
src/                      # (optional) python utils
```
- Notebook paths in `resources/*.yml` are **relative to `resources/`** → use `../notebooks/...`.
- `include: resources/*.yml` in databricks.yml.

## Portability (copy pricing's exactly)
- `databricks.yml` declares `variables: { catalog, schema, warehouse_id, ... }` with defaults.
- Flow: `${var.catalog}` → job/pipeline `base_parameters`/`configuration` → notebook `dbutils.widgets.get(...)` → `fqn = f"{catalog}.{schema}"`. **Never hardcode** catalog/schema/IDs.
- DLT notebooks read `spark.conf.get("source_catalog")` / `spark.conf.get("source_schema")` from the pipeline `configuration` block.
- App reads **env vars** (set in `app.yaml`): `CATALOG_NAME`, `SCHEMA_NAME`, `WAREHOUSE_ID`, endpoint names, Genie IDs, `USE_CACHE`. Helper `fqn(table)`.

## DLT style (mirror claims 01_bronze_dlt_pipeline.py)
- `@dlt.table(name=..., comment=..., table_properties={"quality":"bronze","layer":"bronze"})`.
- Quality: `@dlt.expect("rule","predicate")` (track+retain) + `@dlt.expect_or_drop(...)` (drop).
- **Quarantine**: a mirror table reading from **landing** (the source), filtering the failing predicate, `quarantine_reason` + `_quarantined_at`. One seeded messy bordereau.
- Add `_bronze_ingested_at = current_timestamp()`.

## UC functions (mirror claims 06_agent_tools.py)
- `CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_*(...) RETURNS STRUCT<...>` with a **rich COMMENT** (the supervisor routes off it).
- Scorer fns call `ai_query('{endpoint}', named_struct(...), 'ARRAY<DOUBLE>')` with a pre-fetched feature struct (feature-vector contract; no online store).
- **Resolve endpoint names at fn-creation time** by substring (handles DAB dev-prefix + truncation):
  `EP = next(n for n in [e.name for e in w.serving_endpoints.list()] if "<substring>" in n)`.
- **GOTCHA:** `CREATE OR REPLACE FUNCTION` revokes EXECUTE grants to agent SPs → re-grant / redeploy agents after recreating functions.

## Pricing = reinsurance pricing, NOT GLM (sacred invariant 9)
- Price by **rate-on-line, expected loss, burning cost, exposure/experience rating** over loss bordereaux. Never a primary-insurance frequency-severity-demand GLM.
- **Reuse `pricing-workbench` for app shell + DAB patterns ONLY — never its pricing models** (`freq_glm`/`sev_glm`/`demand_gbm`/rating engine). `model_loss_ratio` here is a burning-cost / experience-rating estimator; `fn_price_submission` returns RoL adequacy + combined ratio.
- **Scope boundary:** go deep on submission→triage→price→accumulation→capital; do NOT build IFRS 17 / QRTs / regulatory reporting (that's the Solvency II demo) — bridge only via the 1-in-200 SCR cross-link; IFRS 17 is a roadmap mention.

## Models + serving + Feature Store (mirror claims 05_ml_models.py)
- Feature Store: `fe.create_training_set(feature_lookups=[FeatureLookup(table_name=fqn('feature_*'), lookup_key='submission_public_id')], label=...)`.
- Log with signature + input_example; `registered_model_name = {catalog}.{schema}.model_*`; set alias `champion`.
- Serving endpoints declared in `resources/serving_endpoints.yml`: `name`, `entity_name=${var.catalog}.${var.schema}.model_*`, `scale_to_zero_enabled: true`, `workload_size: Small`.
- **Feature-vector serving** (no online store) — UC fn pre-fetches features via SQL and passes struct to `ai_query`.

## Agents (mirror claims 06_agents.py + solvency supervisor)
- One agent per persona; deploy via `from databricks import agents; agents.deploy(model_name, version, scale_to_zero=True, environment_vars={"AGENT_WAREHOUSE_ID": wh})`.
- Attach tools via `mlflow.models.resources`: `DatabricksServingEndpoint`, `DatabricksFunction(fqn.fn_*)`, `DatabricksTable`, `DatabricksGenieSpace`, `DatabricksSQLWarehouse`.
- **LLM sub-agents use TABLE-READ tools only** (execute_query) to avoid cross-endpoint CAN_QUERY perm walls (solvency expert-agent pattern).
- **Dev-prefix gotcha:** `agents.deploy` names differ dev vs prod (`agents_<cat>-<sch>-agent_<truncated>`). Resolve by **substring** everywhere.
- Supervisor: managed (Agents UI) captured in a runbook; **narrates only**, never binds. Separate synthesis box in app.
- FM model: `databricks-claude-sonnet-4-6` (Claude via Foundation Model API — per user feedback, use Claude not Llama).

## Cache wrapper (mirror claims agent_cache.py)
- Delta table `cache_agent_responses`; key = sha256(endpoint + canonical_json(input))[:..]; MERGE upsert.
- `get_agent_response(endpoint, input, use_cache=USE_CACHE)` → `{cache: hit|miss|bypass, response}`.
- `USE_CACHE` env flag. **Caches LLM narration ONLY** — never structured decision/price/accumulation/capital outputs (those call UC fns live).
- Reset clears + re-warms both heroes (`sub:900001`, `sub:900002`).

## App (thin — mirror pricing shell, claims deployability)
- FastAPI root `app.py` + `server/{config,sql,agent_client,ai_cache}.py` + `server/routes/*.py`.
- Frontend: **self-contained `app/dist/index.html`** (vanilla, no build step — npm has no egress here). Style to pricing design system: slate-900 (`#1e293b`) sidebar, blue-400/600 accents, amber "About this demo" box, eyebrow + headline cards.
- `app.yaml` uses `valueFrom` (camelCase) for resource-bound values; env block for CATALOG_NAME/SCHEMA_NAME/WAREHOUSE_ID/endpoint names/USE_CACHE. (Databricks Apps port 8000.)
- **Every panel calls a real UC fn / endpoint / Genie / SQL and renders. No logic in app.** Supervisor box → supervisor endpoint via USE_CACHE (pre-warmed).
- App SP grants (post-deploy): `CAN_USE` warehouse; `CAN_QUERY` each serving + agent endpoint; UC `EXECUTE` fns, `SELECT` tables, `USE SCHEMA`.
- Nav: Intake · Transformation · Reinsurance AI · Portfolio & Capital · Governance · Learn.

## Reset + smoke (mirror claims reset_job + 98_smoke_test)
- Reset = multi-task serverless job: data-gen (seed=42, rolling dates to `current_date()`) → bronze full_refresh → silver → gold+features → cache reset+rewarm. retrain=false default.
- App "Reset demo" button triggers job by **substring name**; SP needs `CAN_MANAGE_RUN`.
- Smoke (P10 only): one-row-per-step PASS/FAIL table, fails loudly. Both heroes end-to-end.

## Redeployability audit (solvency P6a — REDEPLOYABILITY_AUDIT.md)
- B1 hardcoded refs · B2 out-of-bundle state · B3 manual steps · B4 external deps · B5 idempotency gaps · B6 verification gaps.

## CDR/MRC extraction (reference insurance_mrc_poc — don't rebuild)
- ACORD entities (Insured/Broker/Insurer/Limit/Deductible/Clause/Policy/Premium) + relationships; `ai_parse_document` → `ai_query` LLM extract → graph_nodes/graph_edges. For reinsurance: MRC slip → submission header + layers. We **mock** the GRLC/CDR-shaped landing (clean structured vs messy manual), reusing the slip shape only.

## Disclaimer (user feedback — every demo)
- "About this demo" box: synthetic universe (Bricksurance Re + synthetic cedants incl. Bricksurance SE); real Databricks objects (DLT, UC FS, MLflow serving, UC fns, agents/Claude, Genie, governance); cat engine + placement network are **external boundaries we ingest from, never rebuild**; everything else illustrative.
- **No "WOW" branding.** Single schema, numbered tables. Push to `wryszka/reinsurance_workbench` (public) after each phase.
