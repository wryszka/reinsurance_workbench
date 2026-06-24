# Bricksurance Re — Reinsurance Intelligence Workbench
### Executive brief · demo run · user stories

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
- **Prove every number** — every headline tile drills to the rows it is computed from, the source UC table/function,
  and the SQL. Nothing is a hardcoded mockup.
- **Govern the whole thing** — one governed copy of the data surfaced many ways by role; real lineage, masking,
  model versions, an AI-activity trail, and a full end-to-end track of any deal — the regulator-readable view.
- **One platform instead of five** — ingestion, pricing, accumulation, capital, AI and governance in one place;
  we own the boundaries around the cat models and placement, we never rebuild them.

**Built on standard Databricks:** Lakeflow Declarative Pipelines (DLT) + Unity Catalog, the Feature Store, MLflow
Model Registry + Model Serving, Unity Catalog functions, a Mosaic AI tool-calling agent (Claude via the Foundation
Model API), Document AI (`ai_query`), AI/BI Genie, and Databricks Apps. *Bricksurance Re is synthetic; cat-model
and capital figures are illustrative — in production these functions integrate the vendor cat models and the
capital engine.*

---

# Part 2 — Demo run (~9 minutes)

**Before the room:** click **↺ Reset demo** (bottom-left) once — it re-anchors the dates to today, restores the
default state, and **re-warms the AI cache** so the agents answer instantly on stage. (Or run the
`reinsurance_98_smoke_test` job — it asserts the whole story end to end.) The first open after a long idle pays a
scale-to-zero warehouse cold-start; the reset/warm step covers it.

**The shape:** Overview → Control Tower (prove it) → Renewal Desk → work the two deals → the storm → Governance → close.

### 0 · Open — the Overview landing  *(frame the whole thing)*
The app opens on the **Overview**: the underwriting cycle end to end as seven clickable stages
(Ingestion → Triage → Price → Accumulation → Capital → Decide → Cat event), the "About this demo" honesty box,
and the three things the platform makes possible. *"One platform, broker-slip to bound treaty to the capital it
consumes."* → click **Open the Control Tower**.

### 1 · Control Tower  *(the CRO's view — and the "is this real?" answer)*
*"Here's the book today."* European windstorm is **RED at 97.6% of appetite** (€488m of a €500m 1-in-200 limit);
CEE flood and US hurricane have headroom. **Solvency II ratio ~181%.**
**The wow that disarms the skeptic:** *"Don't take the tile's word for it — click it."* The €488m tile expands to
**the 23 in-force treaties whose PML contributions sum to exactly €488m**, the three vendor curves it blends, the
source tables, the pipeline build time, and **the actual SQL**. Click the solvency tile → the capital build
(standalone SCR by zone → diversification → BSCR → own funds ÷ SCR = 181.3%). *"Every number drills to the rows
it is computed from — live."* → **Open the renewal desk**.

### 2 · Renewal Desk  *(the underwriter's daily flood)*
Submissions landing for 1-Jan — some via the clean **ADEPT/CDR** feed, some the messy manual path. The "X
quarantined" count clicks through to the rows DLT held back (nothing silently lost). The pain line: *"brokers move
in hours — each one, priced right* and *book-aware?"* Two heroes are pinned at the top.

### 3a · The clean one — `sub:900001`
European **motor quota share**, Bricksurance SE, clean. **Triage: fast-track. Price: adequate** (94% combined).
Accumulation ~zero (motor is away from peak cat), capital **accretive** (RoRAC ~23%). Banner: **RECOMMEND-TO-BIND.**
*"Quote it and move on — speed wins renewals."*

### 3b · The bomb — `sub:900002`  *(the centrepiece)*
European **windstorm cat XoL, €30m xs €20m**, reputable cedant (Helvetia, AA-). *Standalone it looks great* —
**56% combined ratio, adequate price.** Then the crux fires:
- **Accumulation — RED.** The capacity bar **overflows past the appetite line**: +€28.7m of windstorm PML →
  **breaches appetite by €16.7m**, and it's **correlated with 3 in-force treaties** (named).
- **What-if slider** — drag the limit down and the breach re-prices live on `fn_accumulation_whatif`.
- **Capital — RED.** Marginal SCR far exceeds the deal's expected return: **RoRAC 8.7% vs a 15% hurdle —
  capital-destructive.**
- **"Show the UC calls"** — expand it: every card on the page is one Unity Catalog function call
  (`fn_triage_submission`, `fn_price_submission`, `fn_accumulation_impact`, `fn_capital_impact`,
  `fn_recommendation`). Nothing is hardcoded in the app.
- **The AI supervisor** (a real tool-calling agent) returns the call: **REFER** — attractive alone, toxic in
  aggregate. Click **Refer** to log the decision (it MERGEs into the governance audit trail).

### 4 · The wow — Cat Event
*"Overnight, Windstorm Eckhart made landfall in NW Europe."* Open **Cat Event**. In seconds, the book-wide
response: **Solvency II 181% → 141%**, **22 treaties respond**, **gross €150m / net €133m**, reinstatement income,
most-exposed cedant **Helvetia (€50m)**. *Then prove it:* click the **net-loss** tile → the 22 responding treaties
whose ceded losses **sum to the €150m gross**, minus reinstatement = net €133m; click the **solvency gauge** →
own funds €600m − €133m ÷ SCR €331m = 141.2%. *"Days of manual exposure work — here it's one click, and you can
show your working."*

### 5 · Governance  *(the second pillar — what regulators and the board ask for)*
Four tabs, all backed by real Databricks objects:
- **What we collect** — *"we collect everything once, then surface it the way each person needs."* The data
  inventory shows every source, its sensitivity tier, **where it is surfaced and for whom**; plus real UC lineage
  and the column-mask demo (a cedant's PD is **redacted for the app's own service principal — enforced by Unity
  Catalog, not the app**).
- **Decisions & audit** — the submission-to-bind trail; rows click through to the deal track.
- **Models & AI activity** — the real **MLflow Model Registry** versions and `@champion` (triage v5, pricing v3),
  the governed agent roster, and the AI-activity audit (which agent, which UC-function tools, what it concluded).
- **Deal track** — type `sub:900002` → the **full governed lifecycle**, assembled live: arrived → Document-AI
  extraction → triage → price → accumulation (BREACH) → capital (destructive) → the five agents' reasoning →
  decision logged — each stage tagged with its source. *"If the regulator asks how the AI reached REFER, it's all
  in one place."*

### 6 · (Optional, for the data audience) Ingestion
The feed map shows every source by type — structured, vendor cat output, geospatial, a streaming event footprint,
and the hard one: **unstructured MRC slips read by Document AI** (raw slip → extracted fields; a garbled slip is
auto-quarantined). The **quality scorecard is computed live from the pipeline's own DLT event log.**

### 7 · Close — back to the Control Tower
*"We never rebuilt the cat engine or the placement network — we quantified, at the desk and in the moment, what
one more treaty does to the portfolio, and what one storm does to the capital. Every number traces to its source,
and the whole thing is governed. One platform instead of five."*

**Reset between runs:** the **↺ Reset demo** button (bottom-left) re-anchors dates, restores defaults and re-warms
the AI cache.

---

# Part 3 — User stories to wow the audience

Five short, persona-led journeys. Each is a single "moment" you can run in under 90 seconds; pick the two or three
that fit the room.

### Story 1 — "The deal that would have broken the book" *(Underwriter / Head of UW)*
*As an underwriter on the 1-Jan renewal, I want to know if a great-looking deal is safe for the whole book.*
Open `sub:900002`: 56% combined ratio, adequate price — **every instinct says write it.** Then the crux fires —
**+€28.7m into a peak zone already at 97.6%, breaches appetite by €16.7m, RoRAC 8.7% vs a 15% hurdle.** The
supervisor returns **REFER**.
> **Wow:** *the deal that would have quietly broken the book, caught in seconds — before bind, not at year-end.*

### Story 2 — "Prove it" *(CRO / board)*
*As a CRO, when I put a number in front of the board, I have to be able to defend it.*
On the Control Tower, the windstorm zone is RED at 97.6%. A board member asks *"how do you know that's right?"*
Click the tile: it's the **sum of 23 named in-force treaties (= €488m)**, the three vendor curves it blends, the
source tables, the build time, and the SQL.
> **Wow:** *every headline number drills to the live rows it is computed from — no spreadsheet, no black box.*

### Story 3 — "The 6 a.m. storm" *(CRO / Cat manager)*
*As a CRO the morning after a windstorm, I need the book-wide loss and solvency hit now, not in three days.*
Open **Cat Event**: in seconds — **22 treaties respond, gross €150m, net €133m, Solvency II 181% → 141%**, most
exposed Helvetia €50m. Click the net-loss tile → the 22 treaties that sum to it.
> **Wow:** *days of manual exposure work collapsed to one click — with the working shown.*

### Story 4 — "Explain the AI to the regulator" *(Compliance / Chief Risk)*
*As compliance, I have to show exactly how an AI-assisted decision was reached, and prove the data is governed.*
Governance ▸ **Deal track** `sub:900002`: arrived → extracted by Document AI → triaged → priced → accumulation
breach → capital destructive → **five agents' reasoning** → decision logged — each tagged with its source. Then
**Models & AI activity**: the exact champion model version (triage v5) that scored it; and **What we collect**:
the cedant's PD masked by Unity Catalog even from the app itself.
> **Wow:** *full explainability and governance — the regulator-readable view — out of the box, not bolted on.*

### Story 5 — "Ask the book" *(Pricing actuary / analyst)*
*As an actuary, I want to interrogate the portfolio in plain language and get a real, grounded answer.*
On **Reinsurance AI**, ask *"What would diversify the windstorm peak zone?"* — the supervisor decides which Unity
Catalog functions to call, runs them, and shows **which tools it actually invoked**. Every agent tile is a live
link to where it sits in the workbench; "Ask the Portfolio" opens an AI/BI Genie space over the gold marts.
> **Wow:** *a tool-calling agent that runs the real functions and shows its work — not a chatbot guessing.*

### Bonus — "Collect once, surface many ways" *(CDO / data leader)*
*As a data leader, I want one governed copy of the data, not ten extracts.* Governance ▸ **What we collect**: the
same submission is a raw slip for the underwriter, structured data for pricing, a peak-zone PML for the CRO,
features for the models, a tool for the agents, and natural language via Genie — each role sees only what it needs.
> **Wow:** *one source of truth, surfaced seven ways by role — governed, masked, and lineage-tracked.*

---

*Reflects the app as deployed on the dev workspace (`fevm-lr-dev-aws-us`). Numbers are deterministic (seed=42) and
reconcile live; cat-model and capital figures are illustrative.*
