# Bricksurance Re — Reinsurance Intelligence Workbench
### Executive brief + demo run

App: **https://reinsurance-workbench-7474656169654171.aws.databricksapps.com**

---

# Part 1 — Executive brief

**What it is.** A working reinsurance underwriting + capital cockpit, built entirely on Databricks. It answers a
treaty reinsurer's two hardest questions at the moment they matter: *"should we write this submission, given the
whole book?"* and *"a catastrophe just hit — what's our loss and our capital position, now?"*

**The problem it solves.** At renewal, submissions flood the desk and brokers move in hours. To decide well an
underwriter must stitch together the cat models, the in-force book, the exposure data and the capital model —
which today live in five different tools. So they either quote **slow** (and lose the deal) or quote **blind**
(and write the deal that quietly breaks the book). And when a storm actually lands, getting a book-wide loss and
solvency number takes **days**.

**What we show — the flow, in a few steps:**
1. **A submission arrives** — even as an unstructured broker slip; AI reads it into structured data.
2. **Triage & price** — is it in appetite, and is the rate adequate (rate-on-line, burning cost)?
3. **The crux — accumulation & capital** — what does binding *this* deal do to peak-zone PML vs appetite, and
   does it earn its capital (RoRAC vs hurdle)? This is where a good-looking deal is exposed as toxic in aggregate.
4. **Decide** — the AI advises and challenges; a human binds; the decision is logged to an audit trail.
5. **When the storm hits** — one click gives the book-wide loss, reinstatements, and the hit to the solvency ratio.
6. **The CRO control tower** frames it all: capacity vs appetite by peak zone, and capital.

**Why it matters (the value):**
- **Win more renewals** — quote in seconds, not days, without losing control of the book.
- **Avoid the toxic deals** — see the marginal accumulation and capital cost *before* you bind.
- **Respond to events in seconds, not days** — book-wide loss and solvency impact on demand.
- **One platform instead of five** — ingestion, pricing, accumulation, capital, AI and governance in one place;
  we own the boundaries around the cat models and placement, we never rebuild them.

**Built on standard Databricks:** Lakeflow Declarative Pipelines (DLT) + Unity Catalog, the Feature Store, MLflow
Model Serving, Unity Catalog functions, a Mosaic AI tool-calling agent (Claude via the Foundation Model API),
AI/BI Genie, and Databricks Apps. *Bricksurance Re is synthetic; cat-model and capital figures are illustrative —
in production these functions integrate the vendor cat models and the capital engine.*

---

# Part 2 — Demo run (~8 minutes)

**Before the room:** open the app, click through `sub:900001`, `sub:900002`, the **Cat Event** page and the
**Reinsurance AI** ask-box once to warm them up. (Or run the `reinsurance_98_smoke_test` job — it asserts the
whole story end to end.)

**The shape:** Control Tower → Renewal Desk → work the two deals → the storm → close on the Control Tower.

### 1 · Open — Control Tower  *(the CRO's view)*
*"Here's the book today."* European windstorm is **RED at 97.6% of appetite** (€488m of a €500m 1-in-200 limit);
CEE flood and US hurricane have headroom. **Solvency II ratio ~181%.** Renewal-season banner: **submissions open
for the 1-Jan renewal.** Point out the vendor-divergence note ("we blend three cat vendors that disagree").
→ Click **Open the renewal desk**.

### 2 · Renewal Desk  *(the underwriter's daily flood)*
Submissions landing for 1-Jan — some via the clean **ADEPT/CDR** feed, some the messy manual path. The pain line:
*"brokers move in hours — each one, priced right* and *book-aware?"* Two heroes are pinned at the top.

### 3a · The clean one — `sub:900001`
European **motor quota share**, Bricksurance SE, clean. **Triage: fast-track. Price: adequate** (94% combined).
Accumulation ~zero (motor is away from peak cat), capital **accretive** (RoRAC ~23%). Banner: **RECOMMEND-TO-BIND.**
*"Quote it and move on — speed wins renewals."*

### 3b · The bomb — `sub:900002`  *(the centrepiece)*
European **windstorm cat XoL, €30m xs €20m**, reputable cedant (Helvetia, AA-). *Standalone it looks great* —
**56% combined ratio, adequate price.** Then the crux fires:
- **Accumulation — RED.** The capacity bar **overflows past the appetite line**: +€28.7m of windstorm PML →
  **breaches appetite by €16.7m**, and it's **correlated with 3 in-force treaties** (named).
- **What-if slider** — drag the limit down and the breach re-prices live. *"Shrink the line and it just fits —
  but is that line worth writing?"*
- **Capital — RED.** Marginal SCR far exceeds the deal's expected return: **RoRAC 8.7% vs a 15% hurdle —
  capital-destructive.**
- **The AI supervisor** (a real tool-calling agent) returns the call: **REFER** — attractive alone, toxic in
  aggregate. Click **Refer** to log the decision (it lands in the governance audit trail).

### 4 · The wow — Cat Event
*"Overnight, Windstorm Eckhart made landfall in NW Europe."* Open **Cat Event**. In seconds, the book-wide
response: **Solvency II 181% → 141%**, **22 treaties respond**, **gross €150m / net €133m**, reinstatement income,
most-exposed cedant **Helvetia (€50m)**, and the responding-treaty list. *"That's days of manual exposure work —
here it's one click."*

### 5 · (Optional, for the data audience) Ingestion
The feed map shows every source by type — structured, vendor cat output, geospatial, a streaming event footprint,
and the hard one: **unstructured MRC slips read by Document AI** (raw slip → extracted fields; a garbled slip is
auto-quarantined). The **quality scorecard is computed live from the pipeline's own event log.**

### 6 · Close — back to the Control Tower
*"We never rebuilt the cat engine or the placement network — we quantified, at the desk and in the moment, what
one more treaty does to the portfolio, and what one storm does to the capital. One platform instead of five."*

**Reset between runs:** the **↺ Reset demo** button (bottom-left) re-anchors dates and re-warms the heroes.

---

# Part 3 — VIG requirements
*(To be added — drop the Vienna Insurance Group requirements here on this page.)*
