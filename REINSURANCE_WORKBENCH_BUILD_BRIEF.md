# Bricksurance Re — Reinsurance Intelligence Workbench
## Build brief for Claude Code (single source of truth)

> **What this is.** The complete spec + phased build for the reinsurance demo, modelled on `claims_workbench`, `pricing_workbench`, and `solvency-ii-qrt-demo-pnc-agentic` (all in `~/vibe`). Commit this to the repo root and build from it. Target repo: **`wryszka/reinsurance_workbench`**. Catalog: **variable / workspace-default**. Schema (fixed): **`bricksurance_re`**.

---

## 0. How to run this build

- **Run continuously, P0 → P10.** Do not stop for confirmation between phases or stages. Proceed through the whole roadmap. **Only halt if you hit a genuine blocker or cannot satisfy a sacred invariant** — otherwise keep going. Laurence reviews the finished build, not each step.
- **Minimal checks, no mid-build smoke.** Run only light inline sanity checks to avoid compounding broken work (does the table exist, does the function return, does the endpoint respond). **Do not run full smoke tests at each stage.** The single end-to-end pass is P10; the final review is Laurence's.
- **Thin app, real Databricks (hard rule).** The Databricks App is a **presentation layer only** — it authenticates, calls real Databricks objects, and renders. It contains **no business logic, no data transformation, no scoring, no computation**. Every process runs as a real Databricks object: DLT pipelines, UC Feature Store, MLflow models + serving endpoints, UC functions, Agent Bricks / managed supervisor endpoints, Genie, SQL/dashboards, UC governance. Structured panels always call real UC functions / endpoints live. The only caching is a switchable latency aid (`USE_CACHE`) wrapping **LLM narration only** — never the structured decision outputs.
- **Modular as far as possible.** Each phase produces independent, composable modules with clean interfaces. UC functions are atomic and individually callable. Agents are separate serving endpoints composed by the supervisor. The crux (§4) is its own module others call. The app is fully decoupled from compute. Everything config-driven, reusable, and redeployable. No tight coupling between layers — bronze → silver → gold → features → models → agents → app communicate through stable, documented contracts.

---

## 1. Decisions locked as defaults — eyeball the two ★ before shipping

- **Name / universe** — Bricksurance Re, on top of the existing Bricksurance SE universe (SE is one of several synthetic cedants).
- **Scope** — treaty-led. **Both hero submissions are treaty.** Facultative exists only as a secondary surface (volume-triage beat), not in the hero path.
- **Persona framing** — CRO-framed, underwriter-driven. Open and close on the CRO control tower; run the live beat at the underwriter submission decision view.
- ★ **Peak zones** — **European windstorm** (primary; where the bomb lands) + **CEE flood** (secondary). US Atlantic hurricane parameterised as a documented swap.
- **Cat output** — synthetic EP curves, **engine fully abstracted**. Databricks ingests and blends vendor output; never computes cat. Shape as 2–3 vendors that *disagree*. PML at 1-in-100 / **1-in-200** / 1-in-250 (1-in-200 = Solvency II / capital cross-link).
- **Cross-demo link** — narratively real, technically loose. Bricksurance SE is a named cedant with cession bordereaux *shaped like* its book, seeded as **its own tables inside `bricksurance_re`**. **No cross-catalog hard dependency.**
- **Connectivity boundary** — submissions arrive via an **ADEPT/CDR (ACORD GRLC-shaped) inbound mock**. Category-level by default; name "Neuron" only where WTW framing helps.
- **Positioning** — "own the boundaries" (never rebuild cat or placement) + gentle consolidation/TCO (lead with scope, cost as the quiet exhale, aimed at internal point-tool sprawl, never named competitors).
- ★ **The two hero submissions** — `sub:900001` (clean fast-track) and `sub:900002` (the portfolio bomb). Exact profiles in §5. **Sacred and deterministic.** The bomb must read as *attractive standalone* and be *toxic only in aggregate* — so the catch feels earned.

---

## 2. Sacred invariants (never violate)

1. **Redeployable.** Works on any serverless workspace. Catalog via variable; schema fixed `bricksurance_re`; no hardcoded workspace IDs; no out-of-bundle resources.
2. **Deterministic heroes.** `sub:900001` / `sub:900002` seeded deterministically; byte-identical outputs every run.
3. **Thin app / real Databricks.** No logic or compute in the app (see §0). All processes are real Databricks objects.
4. **Modular.** Independent modules, atomic UC functions, separate agent endpoints, decoupled app, stable contracts between layers.
5. **Structured panels never parse LLM prose.** Decision/price/accumulation/capital panels call real UC functions directly. The supervisor narrates in a *separate* box.
6. **Own the boundaries.** Never build a cat engine or a placement network. Ingest from them; enrich around them.
7. **Escalate-not-bind.** Agents advise, challenge, flag, escalate — never bind. Humans decide.
8. **No cross-catalog hard dependency** on the other demos.

---

## 3. Phase roadmap (P0 → P10) — build straight through

Each phase: **goal · deliverables · runs in Databricks as · module boundary.** Stages (A/B/C) are build order, **not stop points** — proceed through them.

**P0 — Scaffold + inventory + data.**
- *A — Inventory:* read `claims_workbench`, `pricing_workbench`, `solvency-ii-qrt-demo-pnc-agentic` in `~/vibe`; extract DAB layout, catalog/schema portability (copy pricing's exactly), DLT/quarantine style, UC Feature Store + MLflow/serving naming, agent mix + cache wrapper, the `pricing_workbench` app shell (UI reference), reset/rolling-dates, Built-on strip, Learn structure, `PROVENANCE_MAPPING.md` format. Write `CONVENTIONS.md`. Mirror, don't invent.
- *B — Scaffold:* DAB bundle mirroring `CONVENTIONS.md`; `config` (catalog variable, schema `bricksurance_re`, endpoint placeholders, `USE_CACHE`); no hardcoded IDs.
- *C — Data:* synthetic generator, deterministic seeds, rolling dates; tables per §5; Bricksurance SE seeded as a cedant in its own tables; in-force portfolio with an as-at snapshot already heavily written in peak European-wind; both heroes seeded; tagging; **Reset Demo** affordance.
- *Module boundary:* data generator and reset are standalone, re-runnable, parameterised.

**P1 — Bronze (DLT).** Ingest the front door — submissions (MRC slip + premium/loss bordereaux + exposure), **GRLC/CDR-shaped landing**; reuse the CDR v3.2 MRC-slip extraction from `insurance-mrc-poc` (reference, don't rebuild); DLT quality gates + **quarantine** (seed one messy bordereau); the **ADEPT/CDR inbound mock** (clean structured inbound vs messy manual path — mock feed only, no placement network). *Runs as:* a DLT pipeline. *Boundary:* bronze tables exposed through a documented schema contract for silver.

**P2 — Silver.** Structure & enrich; join cedant exposure, loss history, cat output, in-force portfolio; produce clean typed entities. *Runs as:* DLT. *Boundary:* silver entities are the stable input contract for features and the crux.

**P3 — Gold + CRO control-tower marts.** Accumulation by peak zone, capacity-vs-appetite, capital, rate adequacy; PML/AEP/OEP at 1-in-100/200/250. *Runs as:* DLT + SQL marts / a Lakeview dashboard. *Boundary:* gold marts are read-only sources for the app and Genie.

**P4 — Feature engineering (UC Feature Store).** Triage + pricing features keyed by `submission_public_id`. *Runs as:* UC Feature Store tables/functions. *Boundary:* feature contract consumed by models and the crux.

**P5 — Models + serving + THE CRUX (§4).** Triage/appetite, technical pricing, and the marginal **accumulation + capital impact** — registered in MLflow, served, deterministic and explicable for both heroes. *Runs as:* MLflow models + serving endpoints, wrapped as atomic UC functions. *Boundary:* the crux is its own UC function/endpoint that everything downstream calls; nothing recomputes it.

**P6 — Agent Bricks (Reinsurance AI).** UC-function tools + LLM sub-agents + Genie + managed supervisor + cache (§7). *Runs as:* real serving endpoints + UC functions + a Genie space + a managed supervisor (Agents UI, captured in a runbook). *Boundary:* each agent is an independent endpoint; the supervisor composes them.

**P7 — Governance.** Lineage, submission-to-bind audit trail, sanctions/ESG checks, sensitivity tiers, capital/Solvency II cross-link (1-in-200). *Runs as:* UC lineage/tags + governed tables/functions. *Boundary:* governance reads existing objects; adds no business logic to the app.

**P8 — App (thin hero artifact).** Surfaces in §6, built on the `pricing_workbench` app shell. **Presentation only** — every panel calls a real UC function / endpoint / Genie / SQL and renders; the supervisor narration box calls the supervisor endpoint (through `USE_CACHE`, pre-warmed). *Boundary:* the app imports nothing from the compute layer except endpoint/function names from `config`.

**P9 — Reset + redeployability hardening.** Rolling-date reset; pre-warm both heroes; redeployability audit (hardcoded IDs, out-of-bundle resources, automation) — the solvency P6a pattern. *Runs as:* a job + audit. *Boundary:* reset is one entrypoint.

**P10 — End-to-end + packaging.** The single full smoke (both heroes), `REINSURANCE_FORUM_DEMO_RUNBOOK.md`, talk track, `REINSURANCE_101_COURSE.md`, `PROVENANCE_MAPPING.md`, Built-on strip. Then report ready for Laurence's review.

---

## 4. The crux — marginal accumulation + capital at submission

The Hitchcock moment's engine and the one genuinely new component. **A real Databricks UC function / serving endpoint** — deterministic, explicable, its own module. For the heroes:
- `sub:900002`: `+X% peak European-wind PML/AEP`, `marginal SCR > deal expected return` (capital-destructive), `correlated with 3 in-force treaties`. Quantified.
- `sub:900001`: ~zero marginal accumulation, adequate rate → recommend-to-bind.

Build and lock in P5. Everything downstream calls it; nothing recomputes it.

---

## 5. Data model + the two heroes (sacred)

Tables: **submissions** (cedant, broker, treaty/fac, proportional/XoL, layers/attachment/limit, LoB, territories, perils, inception, RoL); **cedant exposure & bordereaux** (premium + loss; Bricksurance SE seeded as one cedant's cessions, own tables); **loss history** (as-if, large, cat); **cat output** (per-peril/region EP curves, PML/AEP/OEP, multi-vendor-divergent); **in-force portfolio** (treaties, cessions, accumulations by peak zone, as-at snapshot); **pricing** (technical price, expected loss, capital cost, RoL adequacy); **capital** (SCR contribution, diversification); **counterparties** (credit quality).

**`sub:900001` — clean fast-track.** European Motor Quota Share, reputable cedant, clean bordereaux, adequate rate, peril/territory away from peak cat zones, negligible marginal accumulation, good counterparty → recommend-to-bind.

**`sub:900002` — the portfolio bomb.** European Property-Cat XoL (e.g. €30m xs €20m), reputable cedant, **attractive standalone** (good RoL, clean-ish loss history) — but concentrated in the peak **European-windstorm** zone the book is already heavily written in. Breaches appetite, marginal SCR > expected return, correlated with 3 in-force treaties. Toxic only in aggregate.

---

## 6. App surfaces (thin — call real Databricks, render only)

Nav: **Intake · Transformation · Reinsurance AI · Portfolio & Capital · Governance** (+ Learn). Build the hero section first; stub the rest.

- **CRO control tower** (Portfolio & Capital) — opening/closing frame: peak zones, capacity vs appetite, PML 1-in-100/200/250, capital. Reads gold marts.
- **Submission decision view** (hero) — one view: appetite, technical price, marginal accumulation, marginal capital, + a separate supervisor synthesis box. Each panel = a direct UC-function/endpoint call. For `sub:900002`, accumulation/capital flag red with quantified reasoning.
- **Underwriter queue** — "My Submissions" worklist.
- **Create-a-submission** — presets for the two heroes + a clean random; synchronous scoring via real endpoints.
- **Genie** — "Ask the Portfolio".
- **Governance** — "what's collected" / decision audit.
- **Learn** — Reinsurance 101.

Formatting: money €/£ with commas; RoL/confidence as % (1 dp); PML/SCR consistent.

---

## 7. Agent architecture (pragmatic mix — all real endpoints)

- **UC-function tools** (deterministic scorers, rich COMMENT so the supervisor routes off them): `fn_triage_submission`, `fn_price_submission`, `fn_accumulation_impact`, `fn_capital_impact`, `fn_submission_summary`, `fn_portfolio_position`.
- **LLM sub-agents:** Data Quality (bordereaux/exposure completeness narrative); Challenge / Second-Opinion (argues the other side).
- **Genie** — "Ask the Portfolio".
- **Managed Reinsurance AI supervisor** — Agents UI, captured in a runbook. Narrates only.
- **Cache** — `USE_CACHE`, LLM narration only, both heroes pre-warmed.
- Concentrate on the hero path (triage, pricing, accumulation, capital, challenge). Counterparty/credit and full renewals stubbed/light.

---

## 8. Connectivity boundary + gentle consolidation

- In **Intake**, show a submission arriving via the **ADEPT/CDR (GRLC-shaped) inbound mock** vs the messy manual path. Mock feed only — never a placement network. Reuse the MRC-slip/CDR extraction.
- One **understated** consolidation line in the control tower or Learn, flat, no adjectives: *"This deal normally touches a placement tool, a separate exposure tool, a bordereaux process, a BI stack, and a spreadsheet for the capital call — here it's one platform."* Lead with scope; cost is the exhale; aim at internal point-tool sprawl, never named competitors.

---

## 9. Provenance & Learn

- `PROVENANCE_MAPPING.md` — two-table (canonical vs extension). **Confirm which Databricks accelerators are currently live before anchoring** (insurance lakehouse blueprint, exposure-management/cat patterns) — flag, don't assert.
- "Built on" strip + talk-track one-liner.
- `REINSURANCE_101_COURSE.md` — mirror the `CLAIMS_101` four-part structure (north star; lifecycle stages What/Truths/Problems/Demo-hook; cross-cutting forces; synthesis; problem→hook table; glossary).

---

## 10. End verification (P10 only)

The single full smoke, run once at P10 — both heroes end-to-end in the thin app: `sub:900001` returns recommend-to-bind with ~zero impact; `sub:900002` fires the quantified accumulation + capital flags with the correlated-treaties reasoning; the supervisor synthesis returns from cache; the control tower opens and closes cleanly. Then report ready for Laurence's review.

---

## 11. Kickoff

> **Prompt to Claude Code, in `wryszka/reinsurance_workbench` (this file committed to the root):**
> "Read `REINSURANCE_WORKBENCH_BUILD_BRIEF.md` and build it end to end, P0 → P10, **without stopping between phases**. Mirror the conventions in `~/vibe`. Keep the app a thin layer over real Databricks objects; keep modules independent. Run only light inline checks — no full smoke tests until P10. Halt only on a genuine blocker. Report when it's ready for review."
