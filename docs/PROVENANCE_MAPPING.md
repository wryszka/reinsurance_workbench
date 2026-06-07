# PROVENANCE_MAPPING.md — Reinsurance Intelligence Workbench

How this demo relates to canonical Databricks insurance patterns, and what we extended. Two tables:
**(1) canonical** (patterns this mirrors) and **(2) extension** (what is genuinely new here).

> ⚠️ **Accelerator status — verify before anchoring in a customer conversation.** The Databricks
> insurance lakehouse blueprint and exposure-management / cat patterns referenced below are cited as
> *reference shapes*, not as currently-GA accelerators. Confirm live availability (Solution Accelerators
> page / your SA team) before presenting any as an off-the-shelf asset. The build itself depends on none of them.

## 1. Canonical — patterns mirrored from siblings in `~/vibe` and Databricks references

| Capability | Canonical source (pattern) | How we use it |
|---|---|---|
| Medallion + DLT + quarantine | `claims_workbench` bronze/silver/gold DLT | Bronze front door (submissions, bordereaux, exposure), `@dlt.expect`/`expect_or_drop`, quarantine table |
| Catalog/schema portability | `pricing-workbench` DAB variables → widgets → env | `catalog` variable is the single portability anchor; schema fixed `bricksurance_re` |
| UC Feature Store (feature-vector serving) | `claims_workbench` `feature_*` + `FeatureLookup` | `feature_submission` keyed by `submission_public_id`; no online store |
| MLflow + Model Serving (scale-to-zero) | `claims_workbench` / `pricing-workbench` | `model_triage_classifier`, `model_loss_ratio`; pyfunc, champion alias |
| UC-function tools with rich COMMENT | `claims_workbench` `06_agent_tools` | `fn_triage/price/accumulation/capital/summary/portfolio` — agents route off the COMMENTs |
| Cache-first narration wrapper (`USE_CACHE`) | `claims_workbench` `agent_cache` | Wraps LLM narration only; structured panels always live |
| LLM sub-agents + supervisor (narrate-only) | `solvency-ii-...-agentic` supervisor | Data Quality + Challenge + Supervisor serving endpoints; supervisor never binds |
| Thin app over real objects | `pricing-workbench` app shell | FastAPI + self-contained dist; every panel calls a real UC fn / mart / endpoint |
| Solvency II SCR / 1-in-200 cross-link | `solvency-ii-...-agentic` capital | `gold_capital_position` + `gov_solvency_crosslink` at the 1-in-200 |
| Reset / rolling dates | `claims_workbench` reset job | Re-anchor to `current_date()`, full-refresh DLT, clear cache |
| Redeployability audit (B1–B6) | `solvency-ii-...-agentic` `REDEPLOYABILITY_AUDIT` | `docs/REDEPLOYABILITY_AUDIT.md` |
| MRC-slip / CDR extraction shape | `insurance_mrc_poc` (ACORD GRLC) | Inbound channel mock (ADEPT/CDR vs manual); shape reused, not rebuilt |

## 2. Extension — what is genuinely new in this demo

| New component | Why it is new | Module |
|---|---|---|
| **The crux — marginal accumulation + capital at submission** | Quantifies what binding ONE deal does to peak-zone 1-in-200 PML vs appetite AND to marginal SCR / RoRAC, with correlated-treaty reasoning. Deterministic, explicable, its own UC functions. | `fn_accumulation_impact`, `fn_capital_impact` (05b) |
| **Multi-vendor cat blend (engine abstracted)** | Ingests 2–3 vendor EP curves that *disagree* and blends them; never computes cat. | `cat_vendor_curves` → `gold_cat_blended` |
| **In-force-aware appetite** | Submission scoring is set against an as-at in-force accumulation snapshot heavily written in European windstorm. | `inforce_accumulation`, `gold_portfolio_position` |
| **"Toxic only in aggregate" hero** | `sub:900002` reads attractive standalone (good RoL, clean loss history) yet breaches appetite + is capital-destructive once accumulation + capital are applied. | data generator + crux |
| **CRO-framed control tower** | Opening/closing frame on capacity vs appetite + capital, built only from gold marts. | app Portfolio & Capital |

**Talk-track one-liner:** *"Bricksurance Re never rebuilds the cat engine or the placement network — it owns the boundaries around them: it ingests vendor cat output and broker submissions, then quantifies, at the moment of decision, what one more treaty does to peak-zone accumulation and to the capital that has to stand behind it."*
