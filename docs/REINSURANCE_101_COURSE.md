# REINSURANCE 101 — a working course for the workbench

Mirrors the four-part `CLAIMS_101` structure: **north star → lifecycle stages → cross-cutting forces →
synthesis** (+ a problem→hook table and a glossary). Written for the SA running the demo and the underwriter/CRO watching it.

---

## North star
Reinsurance is insurance for insurers. A **cedant** (an insurer like Bricksurance SE) buys cover from a
**reinsurer** (Bricksurance Re) to smooth volatility and free up capital. The reinsurer's whole job is
**portfolio construction under tail risk**: each deal looks fine alone; the book can still blow up if too much
correlated peak-peril exposure piles up. The north star of this workbench: **make the marginal, portfolio-level
consequence of one more treaty visible at the moment of decision** — accumulation and capital, quantified.

---

## The submission-to-bind lifecycle

### 1. Submission arrives
- **What:** A broker (Aon, Guy Carpenter, …) sends an MRC slip + premium/loss bordereaux + exposure data.
- **Truths:** Structured feeds (ACORD GRLC / ADEPT-CDR) are clean; the manual path is messy and partial.
- **Problems:** Re-keying, missing fields, inconsistent peril codes. Bad rows silently corrupt analysis.
- **Demo hook:** Intake — ADEPT/CDR vs manual, with a quarantined bordereau row.

### 2. Triage / appetite
- **What:** Is this in appetite at all — territory, peril, structure, counterparty, data quality?
- **Truths:** Most volume should fast-track; the few that matter need a human.
- **Problems:** Appetite is judged deal-by-deal, blind to the in-force book.
- **Demo hook:** `fn_triage_submission` → fast_track / refer / decline.

### 3. Technical pricing
- **What:** Burning-cost / exposure rating → technical rate-on-line; compare to offered.
- **Truths:** A good RoL on a single layer can still be the wrong deal for the portfolio.
- **Problems:** Pricing tools rarely see accumulation or capital.
- **Demo hook:** `fn_price_submission` → predicted loss ratio, technical RoL, adequacy.

### 4. Marginal accumulation + capital  ← **THE CRUX**
- **What:** What does binding THIS layer do to peak-zone 1-in-200 PML vs appetite, and to marginal SCR / RoRAC?
- **Truths:** Diversification is the only free lunch; correlation destroys it. The 1-in-200 is the Solvency II link.
- **Problems:** This is exactly where point tools stop — and where the bomb hides.
- **Demo hook:** `fn_accumulation_impact` + `fn_capital_impact` on `sub:900002`.

### 5. Decision + bind
- **What:** Underwriter (with CRO appetite) decides; humans bind, not machines.
- **Truths:** Escalate-not-bind. The AI advises, challenges, escalates.
- **Problems:** Decisions go unrecorded; regulators later ask "why did you write that?"
- **Demo hook:** Reinsurance AI synthesis box + the governance decision audit.

---

## Cross-cutting forces
- **Accumulation & correlation** — the reinsurer's central risk; peak zones (European windstorm, CEE flood) dominate.
- **Capital (Solvency II)** — every deal consumes SCR at the 1-in-200; capital-destructive deals erode the ratio.
- **Vendor model risk** — cat vendors disagree 15–25%; blend, don't trust one.
- **Counterparty credit** — the cedant can default on reinstatement premium; credit quality steps matter.
- **Data quality & connectivity** — structured inbound beats manual; bad data is silent risk.

---

## Synthesis
A reinsurer wins by saying no to attractive-looking, portfolio-toxic deals and yes to boring, diversifying,
capital-efficient ones. `sub:900001` (clean motor QS, away from peak cat, capital-accretive) is the yes;
`sub:900002` (a good-looking European windstorm cat XoL into an already-full zone) is the no. The workbench
makes both calls **legible and quantified** at the desk.

## Problem → demo hook

| Reinsurance problem | Where the demo answers it |
|---|---|
| Messy / re-keyed submissions | Intake (ADEPT/CDR vs manual + quarantine) |
| Appetite judged blind to the book | `fn_triage_submission` + CRO control tower |
| Good RoL, wrong portfolio | `fn_accumulation_impact` (the crux) |
| Deals that don't earn their capital | `fn_capital_impact` (RoRAC vs hurdle) |
| Vendor model disagreement | `gold_cat_blended` divergence band |
| "Why did you write that?" | Governance decision audit |

## Glossary
- **Cedant** — the insurer buying reinsurance. **Treaty** — cover over a whole portfolio (vs **facultative** = one risk).
- **Quota Share / Surplus** — *proportional* (share premium + losses). **XoL (Excess of Loss)** — pays losses in a layer
  (*limit xs attachment*, e.g. €30m xs €20m). **Cat XoL** — XoL for catastrophe peril.
- **RoL (Rate on Line)** — layer premium ÷ layer limit. **Bordereau** — a periodic schedule of premium or losses.
- **PML / AEP / OEP** — probable maximum loss; aggregate/occurrence exceedance probability curves.
- **1-in-200** — the 99.5th-percentile, 1-year loss; the Solvency II capital calibration.
- **SCR / BSCR** — Solvency Capital Requirement / Basic SCR (diversified). **RoRAC** — return on risk-adjusted capital.
- **Accumulation / peak zone** — concentrated correlated exposure (e.g. European windstorm) the book tracks vs appetite.
