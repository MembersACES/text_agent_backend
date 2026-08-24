"""
Testimonial solution content: defaults in code, overridable via JSON file.
Used to populate testimonial document templates (challenge, approach, outcome, dot points, etc.).
"""

import os
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Google Doc ID for the testimonial template. Must be shared with the same Google account
# used by the n8n document-generation workflow (same as EOI/EF generators).
TESTIMONIAL_TEMPLATE_DOC_ID = os.getenv(
    "TESTIMONIAL_TEMPLATE_DOC_ID",
    "1Q1kVW8F3ahYK6nVIoIcPdCWkmU6t0eFj4Le0nCl5FpA",
)

# Directory for override file (next to backend root)
_BACKEND_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_OVERRIDES_PATH = os.path.join(_BACKEND_ROOT, "data", "testimonial_content_overrides.json")

# Solution type ids used by 1st Month Savings / CRM (must match frontend UTILITY_TO_SOLUTION_TYPE)
SOLUTION_TYPE_IDS = [
    "ci_electricity",
    "sme_electricity",
    "ci_gas",
    "sme_gas",
    "waste",
    "resource_recovery",
]

# Human-readable labels for each solution type
SOLUTION_TYPE_LABELS: Dict[str, str] = {
    "ci_electricity": "C&I Electricity Reviews",
    "sme_electricity": "SME Electricity Reviews",
    "ci_gas": "C&I Gas Reviews",
    "sme_gas": "SME Gas Reviews",
    "waste": "Waste Reviews",
    "resource_recovery": "Oil / Resource Recovery",
}

# Optional extra solution types (e.g. case studies like Automated Cleaning Robot, DMA)
EXTRA_SOLUTION_TYPES: Dict[str, str] = {
    "automated_cleaning_robot": "Automated Cleaning Robot",
    "dma": "Direct Metering Agreement",
    "solar_panel_cleaning": "Solar Panel Cleaning",
    "client_endorsement": "Client Endorsement",
    "ghg_roadmap": "GHG Roadmap",
    "solar_review": "Solar Review",
    "gas_discrepancy": "Gas Discrepancy Recovery",
    "electricity_discrepancy": "Electricity Discrepancy",
    "demand_reset": "Demand Reset",
    "cds": "CDS (Container Deposit Scheme)",
}

ALL_SOLUTION_TYPE_IDS = SOLUTION_TYPE_IDS + list(EXTRA_SOLUTION_TYPES.keys())
for k, v in EXTRA_SOLUTION_TYPES.items():
    SOLUTION_TYPE_LABELS[k] = v


def build_testimonial_file_name(
    type_label: str,
    business_name: str,
    *,
    original_upload_basename: Optional[str] = None,
) -> str:
    """
    Human-readable name for CRM / Drive including testimonial category (e.g. DMA, C&I Electricity Reviews).
    Strips characters that are problematic in file names. Max length 512 for DB column.
    """
    def _scrub(s: str) -> str:
        t = (s or "").strip()
        for ch in '\\/:*?"<>|':
            t = t.replace(ch, "-")
        return " ".join(t.split())

    tl = _scrub(type_label) if type_label else ""
    biz = _scrub(business_name) or "Member"

    if original_upload_basename is not None:
        p = Path(original_upload_basename)
        stem = _scrub(p.stem) or "document"
        ext = (p.suffix or "").lower()
        if ext not in (".pdf", ".docx", ".doc"):
            ext = ""
        base = f"{stem}{ext}"
        out = f"Testimonial - {tl} - {base}" if tl else base
    else:
        out = f"Testimonial - {tl} - {biz}" if tl else f"Testimonial - {biz}"

    if len(out) > 512:
        out = out[:509] + "..."
    return out


def _default_content(solution_type_id: str) -> Dict[str, Any]:
    """Default copy per solution type (sensible placeholders)."""
    label = SOLUTION_TYPE_LABELS.get(solution_type_id, solution_type_id.replace("_", " ").title())
    return {
        "solution_type": solution_type_id,
        "solution_type_label": label,
        "key_outcome_metrics": "Cost Savings and Efficiency",
        "key_challenge_of_solution": "Describe the challenge this solution addresses.",
        "key_approach_of_solution": "Describe the approach taken.",
        "key_outcome_of_solution": "Describe the outcome achieved.",
        "key_outcome_dotpoints_1": "",
        "key_outcome_dotpoints_2": "",
        "key_outcome_dotpoints_3": "",
        "key_outcome_dotpoints_4": "",
        "key_outcome_dotpoints_5": "",
        "conclusion": "Summarise the overall result and recommendation.",
        "esg_scope_for_solution": "SCOPE 3",
        "sdg_impact_for_solution": "SDG 7, 12, 13",
    }


# Prebuilt defaults for 1st Month Savings solution types (can be customised per client later)
DEFAULT_CONTENT: Dict[str, Dict[str, Any]] = {}
for st in ALL_SOLUTION_TYPE_IDS:
    DEFAULT_CONTENT[st] = _default_content(st)

# Defaults must stay in sync with frontend src/lib/testimonial-solution-content.ts
DEFAULT_CONTENT["ci_electricity"].update({
    "key_outcome_metrics": "Energy Cost Reduction, Contract Optimisation",
    "key_challenge_of_solution": (
        "The member was paying more than necessary for electricity, with contract terms that were "
        "hard to compare and invoices that did not clearly match what had been agreed."
    ),
    "key_approach_of_solution": (
        "The ACES team reviewed contract structure, usage data and market pricing to identify "
        "savings opportunities and improve commercial terms."
    ),
    "key_outcome_of_solution": (
        "The ACES team handled retailer negotiation on the member's behalf, reconciling proposed "
        "rates to the member's invoices before anything went to the board. The member was not "
        "asked to run the numbers or sit in the retailer meetings."
    ),
    "key_outcome_dotpoints_1": "Lower energy rates and improved contract terms.",
    "key_outcome_dotpoints_2": "Improved billing transparency and structure.",
    "key_outcome_dotpoints_3": "Demand and usage visibility.",
    "key_outcome_dotpoints_4": "Market-aligned pricing.",
    "key_outcome_dotpoints_5": "Ongoing monitoring and support.",
    "conclusion": "A structured electricity review delivers measurable savings and long-term commercial clarity.",
    "esg_scope_for_solution": "SCOPE 2",
    "sdg_impact_for_solution": "SDG 7, 12",
})

DEFAULT_CONTENT["sme_electricity"].update({
    "key_outcome_metrics": "Cost Savings, Simpler Energy Management",
    "key_challenge_of_solution": (
        "The member had little visibility of whether its electricity rates were competitive, "
        "and found the contract and bills time-consuming to interpret."
    ),
    "key_approach_of_solution": (
        "The ACES team benchmarked the member's electricity rates against current market offers "
        "and simplified contract structures."
    ),
    "key_outcome_of_solution": (
        "The ACES team ran the electricity review in the background of a busy SME week, checking "
        "proposed figures against the member's invoices as they went. The owner was not asked to "
        "clear a diary for retailer calls or spreadsheet work."
    ),
    "key_outcome_dotpoints_1": "Competitive market pricing secured.",
    "key_outcome_dotpoints_2": "Simplified contract terms.",
    "key_outcome_dotpoints_3": "Improved billing clarity.",
    "key_outcome_dotpoints_4": "Reduced administrative burden.",
    "key_outcome_dotpoints_5": "Ongoing support.",
    "conclusion": "SME electricity reviews provide straightforward savings and improved cost visibility.",
    "esg_scope_for_solution": "SCOPE 2",
    "sdg_impact_for_solution": "SDG 7, 12",
})

DEFAULT_CONTENT["ci_gas"].update({
    "key_outcome_metrics": "Gas Cost Optimisation, Billing Accuracy",
    "key_challenge_of_solution": (
        "The member could not tell whether invoiced gas charges matched the contracted rates, "
        "and had no independent view of whether the price was still competitive."
    ),
    "key_approach_of_solution": (
        "The ACES team forensically reviewed billing data and contracts alongside a market pricing review."
    ),
    "key_outcome_of_solution": (
        "The ACES team conducted the gas retail negotiation so the member's operations team did "
        "not have to. Each claimed saving was tied to an invoiced charge rather than a rate sitting "
        "on the contract, and only then did the pack go to the board."
    ),
    "key_outcome_dotpoints_1": "Identification of billing discrepancies.",
    "key_outcome_dotpoints_2": "Structured reconciliation process.",
    "key_outcome_dotpoints_3": "Improved cost transparency.",
    "key_outcome_dotpoints_4": "Optimised contract pricing.",
    "key_outcome_dotpoints_5": "Ongoing monitoring.",
    "conclusion": "Gas reviews ensure accurate billing while delivering long-term cost optimisation.",
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 7, 12, 13",
})

DEFAULT_CONTENT["sme_gas"].update({
    "key_outcome_metrics": "Cost Reduction, Simpler Contracts",
    "key_challenge_of_solution": (
        "The member was on a gas contract it had little time to review, with limited visibility "
        "of whether pricing was fair or the bills were accurate."
    ),
    "key_approach_of_solution": (
        "The ACES team ran a market comparison and simplified the contract."
    ),
    "key_outcome_of_solution": (
        "The ACES team sorted gas pricing around the member's existing workload rather than adding "
        "another project to it. The new offer was matched to the member's invoices so the saving "
        "was visible without a finance deep-dive."
    ),
    "key_outcome_dotpoints_1": "Competitive pricing secured.",
    "key_outcome_dotpoints_2": "Simplified billing.",
    "key_outcome_dotpoints_3": "Improved transparency.",
    "key_outcome_dotpoints_4": "Reduced risk of overpayment.",
    "key_outcome_dotpoints_5": "Ongoing support.",
    "conclusion": "SME gas reviews deliver simple, reliable cost savings and improved clarity.",
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 7, 12, 13",
})

DEFAULT_CONTENT["waste"].update({
    "key_outcome_metrics": "Waste Cost Reduction, Operational Efficiency",
    "key_challenge_of_solution": (
        "The member was paying for waste services that no longer matched how the site actually "
        "operated, with little time to check whether frequency and pricing were still right."
    ),
    "key_approach_of_solution": (
        "The ACES team reviewed waste volumes, service frequency and pricing structures."
    ),
    "key_outcome_of_solution": (
        "The ACES team confirmed the current charges from the member's invoices, then changed "
        "collection frequency and service levels with no interruption to site operations. Bins "
        "kept moving while the contract caught up."
    ),
    "key_outcome_dotpoints_1": "Right-sized service levels.",
    "key_outcome_dotpoints_2": "Reduced collection frequency where appropriate.",
    "key_outcome_dotpoints_3": "Improved pricing structures.",
    "key_outcome_dotpoints_4": "Reduced waste-related costs.",
    "key_outcome_dotpoints_5": "Improved operational efficiency.",
    "conclusion": "Waste reviews align services with actual needs, delivering cost and efficiency benefits.",
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 11, 12, 13",
})

DEFAULT_CONTENT["resource_recovery"].update({
    "key_outcome_metrics": "Reduced oil consumption, revenue from used oil, kitchen efficiency",
    "key_challenge_of_solution": (
        "Used cooking oil was treated only as a disposal cost, even though biodiesel pathways can "
        "turn it into revenue, and the kitchen was buying more standard oil than it needed."
    ),
    "key_approach_of_solution": (
        "The ACES team combined resource recovery with a tighter cooking-oil procurement and usage "
        "model: benchmarking current vs optimised supply, aligning fry practices, and embedding "
        "monitoring so usage drops without hurting throughput or plate quality."
    ),
    "key_outcome_of_solution": (
        "The ACES team coordinated collections and supply around kitchen service, not against it. "
        "Pick-ups and replacement stock were timed to service periods, and usage was read from the "
        "member's invoices rather than supplier estimates."
    ),
    "key_outcome_dotpoints_1": "Revenue from used oil instead of disposal-only treatment.",
    "key_outcome_dotpoints_2": "Lower litres used for comparable output.",
    "key_outcome_dotpoints_3": "Improved sales-per-litre and fry-life performance.",
    "key_outcome_dotpoints_4": "Lower cost intensity per dollar of food throughput.",
    "key_outcome_dotpoints_5": (
        "Reduced cleaning burden where cold filtration and blend optimisation support workflows."
    ),
    "conclusion": (
        "The approach stacks resource recovery with blend and behaviour change—so savings appear in "
        "procurement and operations, validated with a bounded before-and-after measurement window."
    ),
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 7, 9, 12, 13",
})

DEFAULT_CONTENT["dma"].update({
    "key_outcome_metrics": "DMA metering cost reduction",
    "key_challenge_of_solution": (
        "The member's metering charges looked high against what similar sites would expect, but the "
        "agreement and invoice line items were difficult to unpick without specialist time."
    ),
    "key_approach_of_solution": (
        "The ACES team forensically reviewed metering line items and the agreement against benchmarks."
    ),
    "key_outcome_of_solution": (
        "The ACES team supplied the specialist metering review the member could not justify hiring "
        "in-house, working from the invoices and the agreement rather than a generic benchmark pack. "
        "Finance received a position that could be signed off without recruiting a metering analyst."
    ),
    "key_outcome_dotpoints_1": "Annual metering spend and net saving identified.",
    "key_outcome_dotpoints_2": "Agreement and invoice-led review—not retail-only benchmarking.",
    "key_outcome_dotpoints_3": "Savings articulated for budgeting and approvals.",
    "key_outcome_dotpoints_4": "Transparent metrics for CFO and committees.",
    "key_outcome_dotpoints_5": "ESG: typically minimal GHG change where savings are metering-commercial.",
    "conclusion": "DMA reviews deliver concise, defensible metering savings on a single-page summary.",
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 12",
})

DEFAULT_CONTENT["automated_cleaning_robot"] = {
    "solution_type": "automated_cleaning_robot",
    "solution_type_label": "Automated Cleaning Robot",
    "key_outcome_metrics": "Labour reduction, repeatable operating savings, safer consistent cleaning",
    "key_challenge_of_solution": (
        "Routine floor cleaning was absorbing staff time the member would rather spend on guests "
        "and revenue-facing work, with no straightforward way to test a better method."
    ),
    "key_approach_of_solution": (
        "The ACES team piloted an autonomous vacuum/mop robot on representative shifts, benchmarked "
        "against incumbent methods (time, quality, rework), then standardised rostering once "
        "performance was proven."
    ),
    "key_outcome_of_solution": (
        "The ACES team ran the trial without pulling staff off shift. Faster floor dry-down reduced "
        "slip exposure versus manual mop cycles."
    ),
    "key_outcome_dotpoints_1": "Measurable reduction in baseline cleaning labour.",
    "key_outcome_dotpoints_2": "Annual labour cost avoidance at agreed rates once hours are contractual.",
    "key_outcome_dotpoints_3": "Faster floor dry-down and slip-risk reduction versus manual mop cycles.",
    "key_outcome_dotpoints_4": "Often lower water use for comparable floor-care outcomes.",
    "key_outcome_dotpoints_5": "Consistent cadence suitable for scaling to additional units or zones.",
    "conclusion": (
        "Phased robot deployment converts a repeatable manual task into a documented annual saving, "
        "with a pathway to replicate once the first asset proves utilisation and coverage."
    ),
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 3, 6, 8, 9, 11, 12",
}

DEFAULT_CONTENT["solar_panel_cleaning"] = {
    "solution_type": "solar_panel_cleaning",
    "solution_type_label": "Solar Panel Cleaning",
    "key_outcome_metrics": "Higher yield after clean plus inspection, reduced grid reliance, GHG avoidance",
    "key_challenge_of_solution": (
        "The member's solar yield had drifted as soiling and minor defects built up, and it was "
        "unclear whether periodic checks alone were enough or whether faults were going unnoticed."
    ),
    "key_approach_of_solution": (
        "The ACES team performed a systematic clean paired with at least a level 1 electrical "
        "inspection, then compared equivalent production intervals (aligned seasonality and "
        "metering) rather than anecdotal spikes."
    ),
    "key_outcome_of_solution": (
        "The ACES team measured the result off the member's own metering, so it is not a supplier's "
        "claim. Like-for-like intervals were used so the comparison could not be dismissed as a "
        "sunny-day spike."
    ),
    "key_outcome_dotpoints_1": "Measured uplift in daily energy harvest post-service.",
    "key_outcome_dotpoints_2": "Indicative dollar benefit from incremental kWh at agreed tariffs.",
    "key_outcome_dotpoints_3": "Estimated grid-energy and emissions displacement from incremental generation.",
    "key_outcome_dotpoints_4": (
        "Confidence the asset has no flagged electrical safety or performance faults post-inspection."
    ),
    "key_outcome_dotpoints_5": "Lower risk of undetected degradation shortening asset life or warranty exposure.",
    "conclusion": (
        "Treating PV as an operating asset—clean plus inspection—helps protect returns: production "
        "recovers relative to baseline, and faults surface before outages or larger losses."
    ),
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 7, 8, 9, 13",
}

DEFAULT_CONTENT["client_endorsement"].update({
    "key_outcome_metrics": "Trusted advice, less admin, a single energy and sustainability partner",
    "key_challenge_of_solution": (
        "The member was dealing with retailers, contractors and invoices in pieces, with no one "
        "owner who could explain what was worth doing and then actually do it."
    ),
    "key_approach_of_solution": (
        "The ACES team sat with the member as an ongoing adviser — reviewing bills and contracts, "
        "prioritising work, and running the conversations with suppliers so staff were not the project managers."
    ),
    "key_outcome_of_solution": (
        "The member had a named team they could call, a clear picture of what had been done, and "
        "less time spent chasing quotes, contracts and follow-up."
    ),
    "key_outcome_dotpoints_1": "One accountable partner across energy, waste and related services.",
    "key_outcome_dotpoints_2": "Retailer and contractor conversations handled on the member's behalf.",
    "key_outcome_dotpoints_3": "Less internal time spent interpreting bills, offers and next steps.",
    "key_outcome_dotpoints_4": "A documented trail of work suitable for board and committee reporting.",
    "key_outcome_dotpoints_5": "A relationship that continues after the first saving is banked.",
    "conclusion": (
        "Client endorsement testimonials capture the service relationship itself: ACES as the member's "
        "energy and sustainability team, not a one-off quote."
    ),
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 8, 12, 17",
})

DEFAULT_CONTENT["ghg_roadmap"].update({
    "key_outcome_metrics": "A realistic net-zero pathway, Scope visibility, board-ready GHG plan",
    "key_challenge_of_solution": (
        "The member needed a credible greenhouse-gas story for committees and stakeholders, but "
        "inventory, baselines and next actions were scattered or incomplete."
    ),
    "key_approach_of_solution": (
        "The ACES team built a GHG roadmap from the member's own activity data, set out Scope 1–3 "
        "priorities, and sequenced practical projects rather than a generic net-zero slogan."
    ),
    "key_outcome_of_solution": (
        "The member received a plan they could defend: what the footprint is, which actions move it, "
        "and what can be done this year versus later."
    ),
    "key_outcome_dotpoints_1": "Inventory structured to Scope 1, 2 and relevant Scope 3.",
    "key_outcome_dotpoints_2": "A sequenced roadmap instead of an unstructured wish-list.",
    "key_outcome_dotpoints_3": "Actions tied to operations the member already runs (energy, waste, oil, plant).",
    "key_outcome_dotpoints_4": "Language and numbers suitable for board, audit and member reporting.",
    "key_outcome_dotpoints_5": "A baseline that later projects (DMA, waste, solar) can report against.",
    "conclusion": (
        "A GHG roadmap turns climate reporting from a one-off document into an operating plan the "
        "member can update as projects land."
    ),
    "esg_scope_for_solution": "SCOPE 1, 2, 3",
    "sdg_impact_for_solution": "SDG 7, 12, 13",
})

DEFAULT_CONTENT["solar_review"].update({
    "key_outcome_metrics": "System performance, tariff fit, whether more solar or storage is justified",
    "key_challenge_of_solution": (
        "The member had solar (or was considering it) but could not tell whether the array was "
        "performing, whether the tariff still fitted, or whether a quoted upgrade would pay back."
    ),
    "key_approach_of_solution": (
        "The ACES team reviewed generation, imports, exports and the current commercial terms, then "
        "compared options against the member's load rather than a generic installer brochure."
    ),
    "key_outcome_of_solution": (
        "The member had a clear finding: keep, maintain, or change the system and tariff, with numbers "
        "that could be shown to a committee."
    ),
    "key_outcome_dotpoints_1": "Generation and grid-use checked against expected performance.",
    "key_outcome_dotpoints_2": "Tariff and export settings reviewed for the actual load profile.",
    "key_outcome_dotpoints_3": "Upgrade or storage claims tested, not taken at brochure value.",
    "key_outcome_dotpoints_4": "Maintenance or cleaning called out only where it changes yield.",
    "key_outcome_dotpoints_5": "A written recommendation suitable for capex discussion.",
    "conclusion": (
        "A solar review is a commercial and technical check of an existing or proposed PV asset — "
        "distinct from a one-off panel clean."
    ),
    "esg_scope_for_solution": "SCOPE 2",
    "sdg_impact_for_solution": "SDG 7, 9, 13",
})

DEFAULT_CONTENT["gas_discrepancy"].update({
    "key_outcome_metrics": "Recovered overcharge, corrected billing, retailer credit",
    "key_challenge_of_solution": (
        "Gas invoices did not match the contract or meter, and the member was absorbing the difference "
        "because reconciling retailer bills is slow and specialist."
    ),
    "key_approach_of_solution": (
        "The ACES team reconstructed what should have been billed from the contract, meter data and "
        "invoice history, then put the discrepancy to the retailer with a documented claim."
    ),
    "key_outcome_of_solution": (
        "The overcharge was quantified and pursued, so the member was not left to argue a technical "
        "billing error without evidence."
    ),
    "key_outcome_dotpoints_1": "Invoice lines checked against contracted rates and meter reads.",
    "key_outcome_dotpoints_2": "The dollar discrepancy stated clearly for finance.",
    "key_outcome_dotpoints_3": "Retailer engagement handled by ACES, not venue staff.",
    "key_outcome_dotpoints_4": "A paper trail if the credit is queried later.",
    "key_outcome_dotpoints_5": "Ongoing bills watched so the same error does not recur.",
    "conclusion": (
        "Gas discrepancy recovery is a billing-correction outcome: money back (or stopped leakage) "
        "where the invoice did not match what was agreed."
    ),
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 12",
})

DEFAULT_CONTENT["electricity_discrepancy"].update({
    "key_outcome_metrics": "Corrected electricity billing, recovered overcharge, cleaner invoices",
    "key_challenge_of_solution": (
        "Electricity invoices were out of step with the contract, meter or agreed adjustments, and "
        "the error was easy to miss in a long tax invoice."
    ),
    "key_approach_of_solution": (
        "The ACES team compared billed rates, quantities and adjustments to the contract and metering, "
        "then raised a documented correction with the retailer."
    ),
    "key_outcome_of_solution": (
        "The member had a quantified adjustment and a corrected billing position, without having to "
        "run the reconciliation in-house."
    ),
    "key_outcome_dotpoints_1": "Rates and quantities checked line by line against the agreement.",
    "key_outcome_dotpoints_2": "Discrepancy value stated for finance and committee reporting.",
    "key_outcome_dotpoints_3": "Retailer correction requested with working papers attached.",
    "key_outcome_dotpoints_4": "Network or metering issues separated from retailer billing errors.",
    "key_outcome_dotpoints_5": "A watch on subsequent invoices to confirm the fix held.",
    "conclusion": (
        "Electricity discrepancy work is about making the bill match the deal — a recoverable error, "
        "not a new procurement."
    ),
    "esg_scope_for_solution": "SCOPE 2",
    "sdg_impact_for_solution": "SDG 7, 12",
})

DEFAULT_CONTENT["demand_reset"].update({
    "key_outcome_metrics": "Lower maximum demand, reduced network charges, reset of a stale demand ratchet",
    "key_challenge_of_solution": (
        "Network charges were still based on a high maximum demand that no longer reflected how the "
        "site ran, so the member was paying for a peak that was history."
    ),
    "key_approach_of_solution": (
        "The ACES team evidenced actual demand, prepared the reset case, and managed the distributor "
        "or retailer process so the ratchet could be reviewed."
    ),
    "key_outcome_of_solution": (
        "Demand was reset (or a clear pathway obtained) so ongoing network charges better matched "
        "current operations."
    ),
    "key_outcome_dotpoints_1": "Historical versus current maximum demand documented.",
    "key_outcome_dotpoints_2": "Network tariff impact of a reset estimated in dollars.",
    "key_outcome_dotpoints_3": "Distributor or retailer process run by ACES.",
    "key_outcome_dotpoints_4": "Operational notes so a one-off spike does not rebuild the ratchet.",
    "key_outcome_dotpoints_5": "A result that shows up on subsequent network invoices.",
    "conclusion": (
        "A demand reset is a network-charge outcome: stop paying for a peak the site no longer sets."
    ),
    "esg_scope_for_solution": "SCOPE 2",
    "sdg_impact_for_solution": "SDG 7, 12",
})

DEFAULT_CONTENT["cds"].update({
    "key_outcome_metrics": "Container refunds captured, less residual waste, a workable CDS process",
    "key_challenge_of_solution": (
        "Eligible drink containers were still going out with general waste or recycling, so refund "
        "value and diversion were being left on the table."
    ),
    "key_approach_of_solution": (
        "The ACES team set up a practical CDS collection path for the venue — what is eligible, where "
        "it sits, and how refunds are claimed — without adding a complex extra roster."
    ),
    "key_outcome_of_solution": (
        "Containers that qualify are separated and claimed, so the member sees both a small revenue "
        "line and a cleaner waste profile."
    ),
    "key_outcome_dotpoints_1": "Eligible containers identified in the existing waste stream.",
    "key_outcome_dotpoints_2": "A collection method that staff can actually run on shift.",
    "key_outcome_dotpoints_3": "Refunds tracked so finance can see the return.",
    "key_outcome_dotpoints_4": "Less eligible material in residual or commingled recycling.",
    "key_outcome_dotpoints_5": "A process that sits beside the broader waste review, not instead of it.",
    "conclusion": (
        "CDS testimonials record a container-deposit outcome: refunds and diversion from a scheme "
        "the venue was previously leaking."
    ),
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 12",
})


def _ensure_data_dir() -> None:
    d = os.path.dirname(_OVERRIDES_PATH)
    if not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)


def _load_overrides() -> Dict[str, Dict[str, Any]]:
    """Load overrides from JSON file. Returns dict keyed by solution_type."""
    if not os.path.isfile(_OVERRIDES_PATH):
        return {}
    try:
        with open(_OVERRIDES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning("Failed to load testimonial content overrides: %s", e)
        return {}


def _save_overrides(overrides: Dict[str, Dict[str, Any]]) -> None:
    """Write overrides to JSON file."""
    _ensure_data_dir()
    with open(_OVERRIDES_PATH, "w", encoding="utf-8") as f:
        json.dump(overrides, f, indent=2, ensure_ascii=False)


def get_merged_content(solution_type_id: Optional[str] = None) -> Any:
    """
    Return merged content (defaults + overrides).
    If solution_type_id is None, return list of merged content for all solution types.
    Otherwise return single merged dict for that solution type.
    """
    overrides = _load_overrides()
    if solution_type_id is not None:
        base = DEFAULT_CONTENT.get(solution_type_id)
        if not base:
            return None
        merged = dict(base)
        if solution_type_id in overrides:
            for k, v in overrides[solution_type_id].items():
                if v is not None and (isinstance(v, str) or not isinstance(merged.get(k), str)):
                    merged[k] = v
        return merged
    result: List[Dict[str, Any]] = []
    for st in ALL_SOLUTION_TYPE_IDS:
        merged = get_merged_content(st)
        if merged:
            result.append(merged)
    return result


def save_override(solution_type_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Save override for one solution type. Only provided keys are updated.
    Returns merged content after save.
    """
    if solution_type_id not in ALL_SOLUTION_TYPE_IDS:
        raise ValueError(f"Unknown solution_type: {solution_type_id}")
    overrides = _load_overrides()
    allowed_keys = {
        "key_outcome_metrics", "key_challenge_of_solution", "key_approach_of_solution",
        "key_outcome_of_solution", "conclusion", "esg_scope_for_solution", "sdg_impact_for_solution",
        "key_outcome_dotpoints_1", "key_outcome_dotpoints_2", "key_outcome_dotpoints_3",
        "key_outcome_dotpoints_4", "key_outcome_dotpoints_5",
    }
    current = overrides.get(solution_type_id) or {}
    for k, v in payload.items():
        if k in allowed_keys and v is not None:
            current[k] = str(v).strip() if isinstance(v, str) else v
    overrides[solution_type_id] = current
    _save_overrides(overrides)
    return get_merged_content(solution_type_id)
