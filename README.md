# Bricksurance Re — Reinsurance Intelligence Workbench

A treaty-led reinsurance demo on Databricks. A CRO control tower over the in-force book, and an underwriter
submission decision view where the **crux** fires: what does binding *one more treaty* do to peak-zone
accumulation and to the capital behind it?

- **Two sacred heroes.** `sub:900001` — clean motor quota share → **recommend-to-bind**. `sub:900002` — a
  European windstorm cat XoL that looks attractive standalone but **breaches appetite and is capital-destructive
  in aggregate** (correlated with 3 in-force treaties).
- **Thin app, real Databricks.** Every panel calls a real DLT mart / UC Feature Store / MLflow serving endpoint /
  UC function / agent / Genie. The app contains no business logic. `USE_CACHE` wraps LLM narration only.
- **Own the boundaries.** We never rebuild the cat engine or the placement network — we ingest vendor cat output
  (blending vendors that disagree) and broker submissions, then quantify accumulation, capital and governance.

## Build & run
See `REINSURANCE_WORKBENCH_BUILD_BRIEF.md` (the spec, P0→P10) and `CONVENTIONS.md`. Deploy order and grants are in
`docs/REDEPLOYABILITY_AUDIT.md` (B3); the demo beat sheet is in `docs/REINSURANCE_FORUM_DEMO_RUNBOOK.md`.

```bash
databricks bundle deploy -t dev -p DEV
databricks bundle run reinsurance_00_setup -t dev      # synthetic data (seed=42)
databricks bundle run reinsurance_medallion -t dev     # bronze → silver → gold (DLT)
databricks bundle run reinsurance_05_ml -t dev         # features + models + serving + the crux
databricks bundle run reinsurance_06_ai -t dev         # UC-function tools + agents + governance
# grant the app SP (see B3), then open the app; reset via reinsurance_99_reset; verify via reinsurance_98_smoke_test
```

Portability: override one variable — `--var catalog=<other_catalog>`. Schema is fixed `bricksurance_re`.
Docs: `docs/REINSURANCE_101_COURSE.md` (reinsurance primer), `docs/PROVENANCE_MAPPING.md` (canonical vs extension).
