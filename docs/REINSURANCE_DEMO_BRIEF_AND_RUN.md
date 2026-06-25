# Bricksurance Re — Reinsurance Intelligence Workbench
### Executive brief · demo run · user stories

App: **https://reinsurance-workbench-7474656169654171.aws.databricksapps.com**

---

# Part 1 — Executive brief

**What it is.** A working reinsurance underwriting + capital cockpit, built entirely on Databricks. It answers a treaty reinsurer's two hardest questions at the moment they matter: *"should we write this submission, given the whole book?"* and *"a catastrophe just hit — what's our loss and our capital position, now?"*

**The problem it solves.** As the 1-Jan renewal nears, submissions flood the desk and the turnaround window shrinks from weeks to days — sometimes hours. To decide well an underwriter must stitch together the cat models, the in-force book, the exposure data and the capital model — which today live in five different tools. So they either quote **slow** (and lose the deal) or quote **blind** (and write the deal that quietly breaks the book). And when a storm actually lands, even a **day-one modelled** loss and solvency number takes **days** to assemble.

**What we show — the flow, in a few steps:**
1. **A submission arrives** — even as a horrendously-formatted MRC slip (often scanned, with handwritten notes); Document AI reads the legible ones into structured data and **auto-quarantines the garbled ones for human review**.
2. **Triage & price** — is it in appetite, and is the rate adequate (rate-on-line, burning cost)?
3. **The crux — accumulation & capital** — what does binding *this* deal do to peak-zone PML vs appetite, and does it earn its capital (RoRAC vs hurdle)? This is where a good-looking deal is exposed as toxic in aggregate.
4. **Decide** — the AI advises and challenges; a human binds; the decision is logged to an audit trail.
5. **When the storm hits** — one click gives the **day-one modelled** loss (event footprint × in-force limits), reinstatements, and the hit to the solvency ratio.
6. **The CRO control tower** frames it all: capacity vs appetite by peak zone, and capital.

**Why it matters (the value):**
- **Win more renewals** — quote in seconds, not days, without losing control of the book.
- **Avoid the toxic deals** — see the marginal accumulation and capital cost *before* you bind.
- **Respond to events in seconds, not days** — a **day-one modelled** book-wide loss and solvency impact on demand.
- **Run your cat output at scale** — vendor cat models emit huge simulated loss tables (**YELTs / PLTs**); we ingest them into Delta and derive the EP curve and the 1-in-200 PML *against the live in-force book* in seconds, not an overnight batch on a workstation (Control Tower ▸ **Cat analytics**).
- **Prove every number** — every headline tile drills to the rows it is computed from, the source UC table/function, and the SQL. Nothing is a hardcoded mockup.
- **Govern the whole thing** — one governed copy of the data surfaced many ways by role; real lineage, masking, model versions, an AI-activity trail, and a full end-to-end track of any deal — the regulator-readable view.
- **One platform instead of five** — ingestion, pricing, accumulation, capital, AI and governance in one place; we own the boundaries around the cat models and placement, we never rebuild them.
- **Orchestrate, don't replace — minutes, not weeks** — the one-line story: we **prep the data, orchestrate your engines, and consume the outputs** so the desk makes sense of them in minutes rather than weeks. Databricks is the high-speed chassis *between* your existing systems: it packages the slip, calls your cat model (Verisk Touchstone) and capital model (WTW Igloo / Tyche), and brings the regulator-approved numbers back to the desk in seconds. Most people believe their incumbent tools are impossible to replace — yet have very real complaints about them. We don't argue the point; we resolve the complaints, because the three things that make the existing stack slow come out of the box here: **data prep** (conform once, not once per tool), **orchestration** (Workflows replace the manual hand-carry between systems), and **governance** (every hop is captured as real Unity Catalog lineage and audited — no extra project). Moving the engines themselves *onto* Databricks can happen organically over time — it is **not** a precondition, and not something to worry about on day one. *(The engine calls are mocked in this demo; the orchestration pattern — Workflows + Auto Loader + UC functions wrapping the APIs — is the production design.)*

**Built on standard Databricks:** Lakeflow Declarative Pipelines (DLT) + Unity Catalog, the Feature Store, MLflow Model Registry + Model Serving, Unity Catalog functions, a Mosaic AI tool-calling agent (Claude via the Foundation Model API), Document AI (`ai_query`), AI/BI Genie, and Databricks Apps. *Bricksurance Re is synthetic; cat-model and capital figures are illustrative — in production these functions integrate the vendor cat models and the capital engine.*

---

# Part 2 — Demo run (~9 minutes)

**Before the room:** click **↺ Reset demo** (bottom-left) once — it re-anchors the dates to today, restores the default state, and **re-warms the AI cache** so the agents answer instantly on stage. (Or run the `reinsurance_98_smoke_test` job — it asserts the whole story end to end.) The first open after a long idle pays a scale-to-zero warehouse cold-start; the reset/warm step covers it.

**The shape:** Overview → Control Tower (prove it) → Renewal Desk → work the two deals → the storm → Governance → close.

> **Credibility watch-outs — say these, an actuary or cat modeller is listening:**
> 1. The cat-event figure is a **day-one *modelled* loss** (the overnight event footprint run against our in-force limits) — *not* actual cedant-reported losses, which aren't known for weeks. Say "modelled."
> 2. The clean motor deal isn't "zero accumulation" — motor carries its own hail / convective-storm accumulation. It's **clear of our peak *windstorm* zone**. Frame it as "doesn't clash with peak windstorm," not "no accumulation."
> 3. **Reinstatements apply to the cat XoL (`sub:900002`), not the motor quota share (`sub:900001`)** — don't conflate them.
> 4. MRC slips really are formatted horrendously (scanned, handwritten) — lean into **auto-quarantine for human review**; the friction is what makes the extraction believable.

### 0 · Open — the Overview landing  *(frame the whole thing)*
The app opens on the **Overview**: the underwriting cycle end to end as seven clickable stages (Ingestion → Triage → Price → Accumulation → Capital → Decide → Cat event), the "About this demo" honesty box, and the three things the platform makes possible. *"One platform, broker-slip to bound treaty to the capital it consumes."* → click **Open the Control Tower**.

### 1 · Control Tower  *(the CRO's view — and the "is this real?" answer)*
*"Here's the book today."* European windstorm is **RED at 97.6% of appetite** (€488m of a €500m 1-in-200 limit); CEE flood and US hurricane have headroom. **Solvency II ratio ~181%.** **The wow that disarms the skeptic:** *"Don't take the tile's word for it — click it."* The €488m tile expands to **the 23 in-force treaties whose PML contributions sum to exactly €488m**, the three vendor curves it blends, the source tables, the pipeline build time, and **the actual SQL**. Click the solvency tile → the capital build (standalone SCR by zone → diversification → BSCR → own funds ÷ SCR = 181.3%). *"Every number drills to the rows it is computed from — live."*

The Control Tower carries four tabs across the top — pick what the room wants: **Capacity & capital** (the drill above), **Business view** (*Ask the Portfolio* in plain language via AI/BI Genie, answered in-app, plus the embedded AI/BI dashboard), **Cat analytics** (the YELT / EP curve — see 1b), and **Executive dashboard** (the AI/BI dashboard embedded full-width). → **Open the renewal desk**.

### 1b · Cat analytics — cat-model output at scale  *(optional, for the cat / actuary room)*
Control Tower ▸ **Cat analytics**. *"This is the workload everyone says is impossible to move."* A real **YELT** (Year Event Loss Table) — **~380k simulated event losses across 100,000 trial years** — landed in **Delta**, then queried **against the live in-force book** (the in-force Cat XoL layers applied to every simulated loss) to derive the **EP curve** and the **1-in-200 PML** — **live, in ~4 seconds**, with the query time and the SQL on show. EU windstorm 1-in-200 OEP **€403m**, under the €500m appetite. *"The simulated loss table that lives on a workstation today — in Delta, queried against the whole book in seconds. The marginal version of this exact query is the crux."*

### 2 · Renewal Desk  *(the underwriter's daily flood)*
Submissions landing for 1-Jan — some via the clean **ADEPT/CDR** feed, some the messy manual path. The "X quarantined" count clicks through to the rows DLT held back (nothing silently lost). The pain line: *"as 1-Jan nears the window shrinks from weeks to days — sometimes hours — and each one has to be priced right* and *book-aware."* Two heroes are pinned at the top.

### 3a · The clean one — `sub:900001`
European **motor quota share**, Bricksurance SE, clean. **Triage: fast-track. Price: adequate** (94% combined). Accumulation **clear** — it doesn't clash with our peak European-windstorm exposure (motor carries its own hail / convective-storm accumulation, but not in *this* peak zone); capital **accretive** (RoRAC ~23%). Banner: **RECOMMEND-TO-BIND.** *"Quote it and move on — speed wins renewals."*

### 3b · The bomb — `sub:900002`  *(the centrepiece)*
European **windstorm cat XoL, €30m xs €20m**, reputable cedant (Helvetia, AA-). *Standalone it looks great* — **56% combined ratio, adequate price.** Then the crux fires:
- **Accumulation — RED.** The capacity bar **overflows past the appetite line**: +€28.7m of windstorm PML → **breaches appetite by €16.7m**, and it's **correlated with 3 in-force treaties** (named).
- **What-if slider** — drag the limit down and the breach re-prices live on `fn_accumulation_whatif`.
- **Capital — RED.** Marginal SCR far exceeds the deal's expected return: **RoRAC 8.7% vs a 15% hurdle — capital-destructive.**
- **Watch the orchestration.** Triggering the crux shows Databricks packaging the slip and calling your engines — *Verisk Touchstone* for the marginal YELT, then *WTW Igloo* for the marginal SCR and RoRAC — and bringing the numbers back. *"What usually takes an underwriter, a cat modeller and an actuary three days, orchestrated in three seconds. We let your engines do the math; we're the chassis connecting them."* (Mocked here; the pattern is real.)
- **"Show the UC calls"** — expand it: every card on the page is one Unity Catalog function call (`fn_triage_submission`, `fn_price_submission`, `fn_accumulation_impact`, `fn_capital_impact`, `fn_recommendation`). Nothing is hardcoded in the app.
- **The AI supervisor** (a real tool-calling agent) returns the call: **REFER** — attractive alone, toxic in aggregate. Click **Refer** to log the decision (it MERGEs into the governance audit trail).

### 4 · The wow — Cat Event
*"Overnight, Windstorm Eckhart made landfall in NW Europe — here's the **day-one modelled** response, the event footprint run against our in-force limits."* Open **Cat Event**. In seconds, the book-wide modelled response: **Solvency II 181% → 141%**, **22 treaties respond**, **gross €150m / net €133m**, reinstatement income, most-exposed cedant **Helvetia (€50m)**. *Then prove it:* click the **net-loss** tile → the 22 responding treaties whose ceded losses **sum to the €150m gross**, minus reinstatement = net €133m; **click any treaty** → why *that* one paid: its layer (attachment → limit), how far the event reached into it, and any reinstatement (with the premium income it brings back); click the **solvency gauge** → own funds €600m − €133m ÷ SCR €331m = 141.2%. *"Days of manual exposure work — here it's one click, and you can show your working at every level."*

### 5 · Governance  *(the second pillar — what regulators and the board ask for)*
Four tabs, all backed by real Databricks objects:
- **What we collect** — *"we collect everything once, then surface it the way each person needs."* The data inventory shows every source, its sensitivity tier, **where it is surfaced and for whom**; plus real UC lineage and the column-mask demo (a cedant's PD is **redacted for the app's own service principal — enforced by Unity Catalog, not the app**).
- **Decisions & audit** — the submission-to-bind trail; rows click through to the deal track.
- **Models & AI activity** — the real **MLflow Model Registry** versions and `@champion` (triage v5, pricing v3), the governed agent roster, and the AI-activity audit (which agent, which UC-function tools, what it concluded).
- **Deal track** — type `sub:900002` → the **full governed lifecycle**, assembled live: arrived → Document-AI extraction → triage → price → accumulation (BREACH) → capital (destructive) → the five agents' reasoning → decision logged — each stage tagged with its source. *"If the regulator asks how the AI reached REFER, it's all in one place."*

### 6 · (Optional, for the data audience) Ingestion — the front door at scale
The big-piece story: a **live storm radar** (track Windstorm Eckhart in near-real-time, the modelled net loss climbing as it develops — *"we consume live geo data and react in minutes, not days"*); the purpose-grouped **source catalogue** (every feed by what it is *for* — submission, model-build, live-event, counterparty, reference — live feeds showing real rows + DQ, new ones flagged mock); the **orchestration DAG** (Databricks as the chassis between Sapiens/SAP, Verisk Touchstone and WTW Igloo — prep → orchestrate → consume, minutes not weeks); the hard one, **unstructured MRC slips read by Document AI** (raw slip → extracted fields; a garbled slip is auto-quarantined for human review); and a **quality scorecard computed live from the pipeline's own DLT event log.** *(The YELT / cat-output-at-scale story lives in Control Tower ▸ Cat analytics — step 1b.)*

### 7 · Close — back to the Control Tower
*"We never rebuilt the cat engine or the placement network — we quantified, at the desk and in the moment, what one more treaty does to the portfolio, and what one storm does to the capital. Every number traces to its source, and the whole thing is governed. One platform instead of five."*

**Reset between runs:** the **↺ Reset demo** button (bottom-left) re-anchors dates, restores defaults and re-warms the AI cache.

---

# Part 3 — User stories to wow the audience

Six short, persona-led journeys (plus a bonus). Each is a single "moment" you can run in under 90 seconds; pick the two or three that fit the room.

### Story 1 — "The deal that would have broken the book" *(Underwriter / Head of UW)*
*As an underwriter on the 1-Jan renewal, I want to know if a great-looking deal is safe for the whole book.* Open `sub:900002`: 56% combined ratio, adequate price — **every instinct says write it.** Hit the crux and watch the orchestration: Databricks packages the structured slip, pushes it to a mocked **Verisk Touchstone** for the marginal windstorm loss, then passes that straight to a mocked **WTW Igloo** engine for the exact capital hit — **+€28.7m into a peak zone already at 97.6%, breaches appetite by €16.7m, RoRAC 8.7% vs a 15% hurdle.** The supervisor returns **REFER**.
> **Wow:** *It orchestrated what usually takes an underwriter, a cat modeller and an actuary three days — in three seconds. We let your engines do the math; we're the high-speed chassis connecting them. The deal that would have quietly broken the book is caught before bind.*

### Story 2 — "Prove it" *(CRO / board)*
*As a CRO, when I put a number in front of the board, I have to be able to defend it.* On the Control Tower, the windstorm zone is RED at 97.6%. A board member asks *"how do you know that's right?"* Click the tile: it's the **sum of 23 named in-force treaties (= €488m)**, the three vendor curves it blends, the source tables, the build time, and the SQL.
> **Wow:** *every headline number drills to the live rows it is computed from — no spreadsheet, no black box.*

### Story 3 — "The 6 a.m. storm" *(CRO / Cat manager)*
*As a CRO the morning after a windstorm, I need a first read on the book-wide loss and solvency hit now, not in three days.* Open **Cat Event**: the overnight event footprint is run against our in-force limits → a **day-one modelled** read, in seconds — **22 treaties respond, gross €150m, net €133m, Solvency II 181% → 141%**, most exposed Helvetia €50m. Click the net-loss tile → the 22 treaties that sum to it. *(Actual cedant-reported losses firm up over weeks; this is the day-one modelled view that lets you act.)*
> **Wow:** *days of manual exposure modelling collapsed to one click — with the working shown.*

### Story 4 — "Explain the AI to the regulator" *(Compliance / Chief Risk)*
*As compliance, I have to show exactly how an AI-assisted decision was reached, and prove the data is governed.* Governance ▸ **Deal track** `sub:900002`: arrived → extracted by Document AI → triaged → priced → accumulation breach → capital destructive → **five agents' reasoning** → decision logged — each tagged with its source. Then **Models & AI activity**: the exact champion model version (triage v5) that scored it; and **What we collect**: the cedant's PD masked by Unity Catalog even from the app itself.
> **Wow:** *full explainability and governance — the regulator-readable view — out of the box, not bolted on.*

### Story 5 — "Ask the book" *(Pricing actuary / analyst)*
*As an actuary, I want to interrogate the portfolio in plain language and get a real, grounded answer.* On **Reinsurance AI**, ask *"What would diversify the windstorm peak zone?"* — the supervisor decides which Unity Catalog functions to call, runs them, and shows **which tools it actually invoked**. Every agent tile is a live link to where it sits in the workbench. And for free-form questions, **Ask the Portfolio** (Control Tower ▸ Business view) answers **in-app** — a real AI/BI Genie over the gold marts, returning the answer, **the SQL it generated**, and the result table.
> **Wow:** *a tool-calling agent that runs the real functions and shows its work — not a chatbot guessing.*

### Story 6 — "Cat output at scale" *(Cat manager / pricing actuary)*
*As a cat manager, the simulated loss tables from the vendor models are enormous and slow to work with — everyone tells me that workload can't move off the specialist tools.* Control Tower ▸ **Cat analytics**: a ~380k-row **YELT** (100,000 trial years) sits in **Delta**; we apply the in-force Cat XoL layers to every simulated loss and read the **EP curve / 1-in-200 PML** off the distribution — **live, in ~4 seconds**, with the SQL on show. EU windstorm 1-in-200 OEP **€403m**, under the €500m appetite.
> **Wow:** *the huge simulated loss table that lives on a workstation today — landed in Delta and queried against the live book in seconds; the marginal version of this query is the crux.*

### Bonus — "Collect once, surface many ways" *(CDO / data leader)*
*As a data leader, I want one governed copy of the data, not ten extracts.* Governance ▸ **What we collect**: the same submission is a raw slip for the underwriter, structured data for pricing, a peak-zone PML for the CRO, features for the models, a tool for the agents, and natural language via Genie — each role sees only what it needs.
> **Wow:** *one source of truth, surfaced seven ways by role — governed, masked, and lineage-tracked.*

---

# Part 4 — Jargon definitions (for the presenter)

Reinsurance has its own vocabulary. These are the terms that come up in the run — defined in plain English for a Databricks SA, not an actuary. (Databricks terms — Genie, Document AI, Unity Catalog, MLflow — are assumed known.)

### The business

| Term | What it means (plain English) |
|------|-------------------------------|
| **Cedant** | The insurer that *buys* reinsurance — it "cedes" (passes on) part of its risk. In this demo, **Bricksurance SE** is a cedant of **Bricksurance Re**. |
| **Reinsurer** | The company that *insures the insurers*. Bricksurance Re. Takes on (assumes) risk in exchange for premium. |
| **Treaty** | A reinsurance contract covering a *whole defined book* of the cedant's policies, agreed up front (vs. one-off "facultative" deals). The unit of business here. |
| **Ceded / assumed** | Two sides of the same coin: the cedant *cedes* risk; the reinsurer *assumes* it. |
| **Submission** | A treaty the cedant's broker shopfronts to reinsurers asking for a quote — the thing landing on the Renewal Desk. |
| **Renewal (1-Jan)** | Most treaties renew annually; 1 January is the big renewal date for European property/cat — hence the "renewal flood." |
| **Broker / MRC slip** | The broker intermediates the placement. The **MRC slip** (Market Reform Contract) is the standard contract document — often a semi-structured PDF, which is why Document AI reads it. |
| **Bordereaux** | Periodic spreadsheets a cedant sends listing premiums or losses policy-by-policy (premium bordereaux / loss bordereaux). A classic messy-data feed. |
| **ADEPT / CDR** | Market data-exchange standards for clean, structured submission/bordereaux feeds (contrast with the messy manual path). |

### How a deal is priced

| Term | What it means |
|------|----------------|
| **Quota share (proportional)** | The reinsurer takes a fixed % of every premium and every loss (e.g. 30%). `sub:900001` is a motor quota share. |
| **Excess of loss (XoL)** | Non-proportional: the reinsurer pays only the part of a loss *above* an attachment, up to a limit. |
| **Layer · "€30m xs €20m"** | An XoL layer: pays losses **in excess of €20m** (the **attachment**), up to **€30m** more (the **limit**). So it covers €20m–€50m. |
| **Cat XoL** | Excess-of-loss covering *catastrophe* events (windstorm, flood, quake). `sub:900002` is an EU windstorm cat XoL. |
| **Rate-on-line (RoL)** | Premium ÷ limit — the headline price of an XoL layer. A €3m premium on a €30m limit = 10% RoL. |
| **Burning cost** | Pricing from history: average past losses to the layer ÷ premium base. The "experience" half of pricing. |
| **Loss ratio** | Losses ÷ premium. Below 100% = underwriting profit on losses alone. |
| **Combined ratio** | Loss ratio + expense ratio. **Below 100% = profitable; above = losing money.** `sub:900002` at 56% looks great in isolation. |
| **Reinstatement** | After an XoL layer pays out, the cedant pays a **reinstatement premium** to restore the cover for the rest of the year — extra income to the reinsurer, which is why net loss < gross loss. |

### Catastrophe & accumulation (the crux)

| Term | What it means |
|------|----------------|
| **Peril** | The cause of loss — windstorm, flood, hurricane, earthquake. |
| **Peak zone** | A geography × peril where exposure concentrates (e.g. *European windstorm*). The reinsurer sets a capacity limit per peak zone. |
| **Accumulation** | The total exposure that piles up in one peak zone across *all* treaties — the thing a single deal can quietly blow up. The demo's central idea. |
| **PML (Probable Maximum Loss)** | The modelled loss at a chosen severity. Here, the loss the book would take from a 1-in-200-year event in a zone. |
| **1-in-200 / return period** | A 1-in-200-year event = the **99.5th percentile** annual loss. It's the Solvency II calibration point. "1-in-100/200/250" are different severities. |
| **EP curve (AEP / OEP)** | Exceedance-Probability curve from a cat model: loss vs. annual probability. **AEP** = aggregate (all events in a year); **OEP** = occurrence (the single biggest event). We *ingest and blend* these from vendors — we don't run the cat model. |
| **YELT / PLT** | **Year / Period Event Loss Table** — the raw simulated output of a cat model: one row per simulated event in a simulated year, with its loss. Huge (often hundreds of millions of rows). You derive the EP curve by applying your contract terms to it and reading the percentiles — exactly what Control Tower ▸ Cat analytics does in Delta. |
| **Cat vendor** | Third parties (e.g. Verisk/AIR, RMS/Moody's) who run the physical cat models and sell the EP curves. We blend three that disagree. |
| **Appetite / capacity / headroom** | **Appetite** = the max PML the reinsurer is willing to hold in a zone; **capacity/headroom** = how much room is left before it's breached. |
| **CRESTA** | Standardised geographic accumulation zones (postcode-ish grids) the industry uses to aggregate exposure. |
| **TIV (Total Insured Value)** | The total value insured at a location/zone — the exposure base for accumulation. |

### Capital & solvency

| Term | What it means |
|------|----------------|
| **Solvency II** | The EU regulatory capital regime for (re)insurers. Drives the headline solvency ratio. |
| **SCR (Solvency Capital Requirement)** | The capital a (re)insurer must hold to survive a 1-in-200-year year. |
| **BSCR / diversification benefit** | **Basic SCR** = the SCR aggregated across risks; because peak zones aren't perfectly correlated, the total is *less* than the sum of standalone SCRs — that reduction is the **diversification benefit**. |
| **Own funds** | The capital actually available to absorb losses (≈ tier-1 capital). |
| **Solvency II ratio** | **Own funds ÷ SCR.** Above 100% = solvent; regulators watch the buffer. 181% in the demo; a storm drops it to 141%. |
| **RoRAC** | **Return on Risk-Adjusted Capital** — the deal's expected return ÷ the marginal capital it consumes. Compared against a **hurdle** (15% here). Below the hurdle = capital-destructive even if the price looks fine. |
| **IFRS 17** | The accounting standard for insurance contracts (a *reporting* concern). Deliberately **out of scope** here — it lives in the Solvency II demo; we bridge only via the 1-in-200 SCR. |

---

*Reflects the app as deployed on the dev workspace (`fevm-lr-dev-aws-us`). Numbers are deterministic (seed=42) and reconcile live; cat-model and capital figures are illustrative.*
