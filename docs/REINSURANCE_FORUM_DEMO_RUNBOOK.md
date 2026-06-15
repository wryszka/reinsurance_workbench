# REINSURANCE_FORUM_DEMO_RUNBOOK.md

CRO-framed, underwriter-driven. Open and close on the CRO control tower; run the live beat at the underwriter
submission decision view. ~8 minutes.

## Pre-flight (once, before the room)
1. `databricks bundle deploy -t dev -p DEV`
2. Run jobs in order: `reinsurance_00_setup` → pipeline `reinsurance_medallion` → `reinsurance_05_ml`
   (FS→model lineage + crux + event engine + `fn_recommendation`) → `reinsurance_06_ai` (specialist narrators +
   governance incl. the UC masking view) → **`reinsurance_06b_agent`** (the REAL tool-calling supervisor agent).
3. Genie: `python3 scripts/create_genie_space.py DEV` → set `GENIE_SPACE_ID` (already wired: `01f168f7…`).
4. Deploy the app: `databricks bundle deploy -t dev`; then `databricks apps deploy reinsurance-workbench`.
5. **Grant the app SP**: `CAN_USE` warehouse; `CAN_QUERY` on every reinsurance serving + agent endpoint **including
   the tool-calling agent** (`agents_…-reinsurance_agent`); `USE CATALOG/SCHEMA`+`SELECT`+`EXECUTE`+`MODIFY`+`CREATE TABLE`
   on `bricksurance_re`; `CAN_MANAGE_RUN` on `reinsurance_99_reset`. (REDEPLOYABILITY_AUDIT.md B3/4b.)
6. **Pre-warm**: open `sub:900001`, `sub:900002`, the Cat Event and the AI ask-box, or run `reinsurance_98_smoke_test`.
   The **supervisor is a real Mosaic AI tool-calling agent** (`reinsurance_agent`) — it calls the UC functions itself;
   the Reinsurance AI ask-box shows the live tool-call trace. No Agents-UI step needed.

## The beat sheet — control-tower → renewal desk → work-an-item → cat event → close (~8 min)
1. **Open — CRO Control Tower.** "Here's the book today." European windstorm is **RED at ~98% of appetite**; CEE
   flood and US hurricane have headroom (capacity remaining shown). Capital: diversified BSCR, Solvency II ratio.
   Renewal-season banner: **29 open submissions** for the 1-Jan renewal. Note the vendor divergence band (3 vendors
   that disagree). Drop the consolidation line. → click **Open the renewal desk**.
2. **Renewal Desk — the daily flood.** Submissions landing for 1-Jan (17 in this week); some via the clean ADEPT/CDR
   feed, some the messy manual path (quarantined rows). Status chips (new/quoted/bound/declined). The pain line:
   *"brokers move in hours — each one, priced right AND book-aware?"* Two heroes pinned at the top.
3a. **The clean one — `sub:900001`.** Motor quota share, Bricksurance SE, ADEPT/CDR. Triage **fast_track**, price
   **adequate**, accumulation ~zero (capacity bar barely moves), capital **accretive** (RoRAC ~23%). Banner:
   **recommend-to-bind**. "Quote it and move on — speed wins renewals."
3b. **The bomb — `sub:900002`.** *Standalone it looks great:* EU property cat XoL €30m xs €20m, Helvetia AA-,
   adequate price (56% combined). Then the crux fires:
   - **Accumulation RED** — the **capacity bar overflows past the appetite line**; +€29m PML, **breaches by ~€17m**,
     **3 correlated in-force treaties light up as red chips** (TR-EUW-101/102/103).
   - **What-if slider** — drag the limit down: utilisation and breach **re-price live**. At €10m it just fits — "but is €10m worth writing?"
   - **Capital RED** — marginal SCR ≫ expected return, **RoRAC ~9% vs 15% hurdle — capital-destructive.**
   - **Portfolio agent** proposes the diversifying alternative (US hurricane, ~19.5% RoRAC); **Counterparty agent**
     confirms Helvetia is clean *(open any Vistula submission to see it inject the regulatory-watch signal)*;
     the **Supervisor** box is a **real Mosaic AI tool-calling agent** — it calls `fn_accumulation_impact`,
     `fn_capital_impact`, `fn_recommendation` etc. itself and the box shows which tools it called. Click **Refer**
     to log the decision → it MERGEs into `gov_decision_audit` (the trail is the artifact).
   - **Reinsurance AI page:** ask the agent live (*"Should we bind sub:900002?"*) and watch the tool-call trace.
3c. **Ingestion (optional deep-dive for the data crowd).** Open **Ingestion**. The feed/source map shows seven
   feeds by *type* — structured, vendor cat output, geospatial, a streaming JSON event footprint, and the hard one:
   **unstructured MRC slips read by Document AI**. The **quality scorecard is computed live from the DLT event
   log** (15 expectations, ~99% pass, 7 quarantined). The **Document AI panel** shows a raw slip → `ai_query`-
   extracted fields; the degraded slip (conf 0.0) is **quarantined by the confidence gate**. Schema-drift (a
   cedant's renamed columns → `_rescued_data`) and H3 geospatial accumulation round it out. "Every feed gated;
   nothing silently lost; even the PDFs become data."
4. **THE WOW — Cat Event.** *"Overnight, Windstorm Eckhart made landfall in NW Europe."* Open **Cat Event**. In
   seconds, the book-wide response: **Solvency II 181% → 141%** (gauge), **22 treaties respond**, gross €150m,
   reinstatement income €17m, **net €133m**, most-exposed cedant **Helvetia €50m**, and the responding-treaty table.
   The **Cat-Event agent** briefs the CRO. "This is days of manual exposure work — here it's one click."
5. **Close — back to the Control Tower.** "Own the boundaries: we never rebuilt the cat engine or the placement
   network — we quantified, at the desk and in the moment, what one more treaty does to the portfolio, and what one
   storm does to the capital. One platform instead of five."

## Reset between runs
Click **↺ Reset demo** (or run `reinsurance_99_reset`). Re-anchors dates to today, full-refreshes the medallion,
rebuilds features + governance, clears the narration cache. Re-warm by opening the two heroes.

## Documented swaps
- **Peak zone swap:** US Atlantic hurricane is parameterised (`ZONE_BASE_PML_1IN200`, `PEAK_ZONES`) — swap which
  zone is "near appetite" by changing `ZONE_CURRENT_PML_TARGET` in `00_setup_and_data_generation.py`.
- **Hero profiles:** `sub:900001` / `sub:900002` are hand-seeded in the generator (§5) — sacred; edit there only.
