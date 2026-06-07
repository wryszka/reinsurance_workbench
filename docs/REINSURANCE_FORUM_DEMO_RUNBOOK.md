# REINSURANCE_FORUM_DEMO_RUNBOOK.md

CRO-framed, underwriter-driven. Open and close on the CRO control tower; run the live beat at the underwriter
submission decision view. ~8 minutes.

## Pre-flight (once, before the room)
1. `databricks bundle deploy -t dev -p DEV`
2. Run jobs in order (or rely on a prior build): `reinsurance_00_setup` → pipeline `reinsurance_medallion` →
   `reinsurance_05_ml` → `reinsurance_06_ai`.
3. Deploy the app: `databricks bundle deploy -t dev` includes the app; then `databricks apps deploy reinsurance-workbench`.
4. **Grant the app SP** (after first app create): `CAN_USE` on the warehouse, `CAN_QUERY` on the 5 reinsurance
   endpoints, `USE SCHEMA` + `SELECT` + `EXECUTE` on `bricksurance_re`, `CAN_MANAGE_RUN` on `reinsurance_99_reset`.
   (See REDEPLOYABILITY_AUDIT.md B3.) Set `GENIE_SPACE_ID` in app.yaml once the Genie space exists.
5. **Pre-warm**: open `sub:900001` then `sub:900002` in the app (warms the supervisor/challenge/dataquality cache),
   or run `reinsurance_98_smoke_test`.
6. Optional managed supervisor: in the Agents UI, create a supervisor over the UC functions + sub-agents; set
   `supervisor_endpoint` var and `EP_SUPERVISOR_SUBSTR`. Without it, the app uses the `reinsurance-supervisor` endpoint.

## The beat sheet
1. **Open — CRO control tower (Portfolio & Capital).** "Here's the book today." European windstorm is **RED at
   ~98% of appetite**; CEE flood and US hurricane have headroom. Capital: diversified BSCR, Solvency II ratio.
   Note the vendor divergence band (we blend 3 vendors that disagree). Drop the consolidation line.
2. **The clean one — `sub:900001`.** Underwriter queue → open it. Motor quota share, Bricksurance SE, ADEPT/CDR
   clean. Triage **fast_track**, price adequate, accumulation ~zero (away from peak cat), capital **accretive**
   (RoRAC above hurdle). Banner: **recommend-to-bind**. "This is the boring, good business."
3. **The bomb — `sub:900002`.** Open it. *Standalone it looks great:* European property cat XoL €30m xs €20m,
   reputable cedant (Helvetia, AA-), good RoL, clean-ish loss history. Then the crux fires:
   - **Accumulation panel RED** — adds ~€29m to European-windstorm 1-in-200 PML, **breaches appetite by ~€17m**,
     **correlated with 3 in-force treaties** (named).
   - **Capital panel RED** — marginal SCR ≫ expected return, **RoRAC ~9% vs 15% hurdle — capital-destructive.**
   - **Supervisor synthesis** narrates the one-line call: **refer / decline** — attractive alone, toxic in aggregate.
4. **Challenge + governance.** Show the Challenge agent arguing the other side; show the decision audit trail
   (what was recommended, who decided, bound or not). "Escalate-not-bind — the human decides; the trail is kept."
5. **Close — back to the control tower.** "Own the boundaries: we never rebuilt the cat engine or the placement
   network — we quantified, at the desk, what one more treaty does to the portfolio and the capital behind it.
   One platform instead of five."

## Reset between runs
Click **↺ Reset demo** (or run `reinsurance_99_reset`). Re-anchors dates to today, full-refreshes the medallion,
rebuilds features + governance, clears the narration cache. Re-warm by opening the two heroes.

## Documented swaps
- **Peak zone swap:** US Atlantic hurricane is parameterised (`ZONE_BASE_PML_1IN200`, `PEAK_ZONES`) — swap which
  zone is "near appetite" by changing `ZONE_CURRENT_PML_TARGET` in `00_setup_and_data_generation.py`.
- **Hero profiles:** `sub:900001` / `sub:900002` are hand-seeded in the generator (§5) — sacred; edit there only.
