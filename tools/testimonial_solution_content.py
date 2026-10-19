"""
Testimonial solution content: defaults in code, overridable via JSON file.
Used to populate testimonial document templates (challenge, approach, outcome, dot points, etc.).
"""

import os
import json
import logging
import re
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
    "vas": "Value Added Service",
    "solar_panel_cleaning": "Solar Panel Cleaning",
    "client_endorsement": "Client Endorsement",
    "association_endorsement": "Association Endorsement",
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

# Family printed on the testimonial as {solution_family}. Types omitted here
# (endorsements, GHG Roadmap, staff-created types) send a blank family.
SOLUTION_FAMILY_LEAD_INS: Dict[str, str] = {
    "Profile Reset": (
        "Starting with a full review of existing agreements, "
        "to identify opportunities in Profile Resets."
    ),
    "Asset Optimisation": (
        "Starting with a review of how the existing asset is performing, "
        "to identify opportunities in Asset Optimisation."
    ),
    "Resource Recovery": (
        "Starting with a review of the current material stream, "
        "to identify opportunities in Resource Recovery."
    ),
}

SOLUTION_TYPE_FAMILIES: Dict[str, str] = {
    "ci_electricity": "Profile Reset",
    "sme_electricity": "Profile Reset",
    "ci_gas": "Profile Reset",
    "sme_gas": "Profile Reset",
    "waste": "Profile Reset",
    "dma": "Profile Reset",
    "vas": "Profile Reset",
    "gas_discrepancy": "Profile Reset",
    "electricity_discrepancy": "Profile Reset",
    "demand_reset": "Profile Reset",
    "resource_recovery": "Resource Recovery",
    "cds": "Resource Recovery",
    "solar_panel_cleaning": "Asset Optimisation",
    "solar_review": "Asset Optimisation",
    "automated_cleaning_robot": "Asset Optimisation",
}


def solution_family_fields(solution_type_id: str) -> Dict[str, str]:
    """Placeholder values for {solution_family} and {solution_family_lead_in}."""
    family = SOLUTION_TYPE_FAMILIES.get((solution_type_id or "").strip(), "")
    return {
        "solution_family": family,
        "solution_family_lead_in": SOLUTION_FAMILY_LEAD_INS.get(family, ""),
    }


def _norm_solution_label(text: str) -> str:
    lowered = (text or "").strip().lower().replace("&", " and ")
    return " ".join(re.sub(r"[^a-z0-9]+", " ", lowered).split())


def solution_type_id_from_label(label: str) -> Optional[str]:
    """Map a sheet/UI type label (or id) to testimonial_solution_type_id."""
    wanted = _norm_solution_label(label)
    if not wanted:
        return None
    if label.strip() in SOLUTION_TYPE_LABELS:
        return label.strip()
    for type_id, type_label in SOLUTION_TYPE_LABELS.items():
        if _norm_solution_label(type_id) == wanted or _norm_solution_label(type_label) == wanted:
            return type_id
    return None


def resolve_testimonial_type(
    db: Any,
    solution_type_id: Optional[str] = None,
    type_label: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    """Map a PATCH payload to (solution_type_id, display label). Blank both → uncategorised."""
    clean_id = (solution_type_id or "").strip() or None
    clean_label = (type_label or "").strip() or None
    if not clean_id and not clean_label:
        return None, None
    wanted = clean_id or clean_label or ""
    mapped = solution_type_id_from_label(wanted)
    if not mapped and clean_label and clean_id:
        mapped = solution_type_id_from_label(clean_label)
    if mapped:
        merged = get_merged_content(mapped, db)
        label = None
        if isinstance(merged, dict):
            label = str(merged.get("solution_type_label") or "").strip() or None
        return mapped, label or SOLUTION_TYPE_LABELS.get(mapped) or clean_label or mapped
    if clean_id:
        merged = get_merged_content(clean_id, db)
        if isinstance(merged, dict):
            label = str(merged.get("solution_type_label") or "").strip() or None
            return clean_id, label or clean_label or clean_id
    return None, clean_label


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
    "key_outcome_metrics": "Lower energy cost, better contract terms",
    "key_challenge_of_solution": (
        "The member was paying more than necessary for electricity, with contract terms that were "
        "hard to compare and invoices that did not clearly match what had been agreed."
    ),
    "key_approach_of_solution": (
        "The ACES team reviewed the contract, usage and market price, and reset the commercial terms."
    ),
    "key_outcome_of_solution": (
        "The ACES team handled retailer negotiation on the member's behalf, reconciling proposed "
        "rates to the member's invoices before anything went to the board. The member was not "
        "asked to run the numbers or sit in the retailer meetings."
    ),
    "key_outcome_dotpoints_1": "Lower rates and better contract terms.",
    "key_outcome_dotpoints_2": "Clearer billing structure.",
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
        "The ACES team benchmarked the rates against current offers and simplified the contract."
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
    "key_outcome_metrics": "Lower waste cost, right-sized service",
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
    "key_outcome_dotpoints_2": "Fewer collections where it still works.",
    "key_outcome_dotpoints_3": "Improved pricing structures.",
    "key_outcome_dotpoints_4": "Reduced waste-related costs.",
    "key_outcome_dotpoints_5": "Improved operational efficiency.",
    "conclusion": "Waste reviews align services with actual needs, delivering cost and efficiency benefits.",
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 11, 12, 13",
})

DEFAULT_CONTENT["resource_recovery"].update({
    "key_outcome_metrics": "Used-oil revenue and lower oil use",
    "key_challenge_of_solution": (
        "Used cooking oil was only a disposal cost, and the kitchen was buying more oil than it needed."
    ),
    "key_approach_of_solution": (
        "The ACES team paired oil recovery with a tighter supply model, without hurting plate quality."
    ),
    "key_outcome_of_solution": (
        "The ACES team timed collections and new stock around kitchen service. Usage was taken from "
        "the invoices, not supplier estimates."
    ),
    "key_outcome_dotpoints_1": "Revenue from used oil, not disposal.",
    "key_outcome_dotpoints_2": "Lower litres for the same output.",
    "key_outcome_dotpoints_3": "Better sales per litre and fry life.",
    "key_outcome_dotpoints_4": "Lower oil cost per dollar of food.",
    "key_outcome_dotpoints_5": "Less cleaning from better oil care.",
    "conclusion": "Oil recovery plus a tighter supply model, checked against the invoices.",
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 7, 9, 12, 13",
})

DEFAULT_CONTENT["dma"].update({
    "key_outcome_metrics": "DMA metering cost reduction",
    "key_challenge_of_solution": (
        "Metering charges looked high, and the agreement was hard to unpick without specialist time."
    ),
    "key_approach_of_solution": (
        "The ACES team forensically reviewed metering line items and the agreement against benchmarks."
    ),
    "key_outcome_of_solution": (
        "The ACES team reviewed metering charges from the invoices and the agreement, so finance "
        "could sign off without a specialist."
    ),
    "key_outcome_dotpoints_1": "Annual metering spend and saving.",
    "key_outcome_dotpoints_2": "Review led by the agreement and invoices.",
    "key_outcome_dotpoints_3": "Savings set out for approval.",
    "key_outcome_dotpoints_4": "Clear figures for finance.",
    "key_outcome_dotpoints_5": "The saving is commercial, not a GHG cut.",
    "conclusion": "DMA reviews deliver concise, defensible metering savings on a single-page summary.",
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 12",
})

DEFAULT_CONTENT["vas"].update({
    "key_outcome_metrics": "VAS service cost reduction",
    "key_challenge_of_solution": (
        "VAS charges sat on the metering invoices and were hard to separate from meter rental."
    ),
    "key_approach_of_solution": (
        "The ACES team isolated VAS line items from meter rental and reviewed them against typical "
        "service-charge benchmarks."
    ),
    "key_outcome_of_solution": (
        "The ACES team separated the VAS charge from meter rental using the invoices. Finance could "
        "sign off the service charge without a specialist."
    ),
    "key_outcome_dotpoints_1": "Annual VAS spend and the saving.",
    "key_outcome_dotpoints_2": "VAS separated from meter rental.",
    "key_outcome_dotpoints_3": "Savings set out for approval.",
    "key_outcome_dotpoints_4": "Clear figures for finance.",
    "key_outcome_dotpoints_5": "The saving is commercial, not a GHG cut.",
    "conclusion": "VAS reviews isolate the service charge and show a defensible saving.",
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 12",
})

DEFAULT_CONTENT["automated_cleaning_robot"] = {
    "solution_type": "automated_cleaning_robot",
    "solution_type_label": "Automated Cleaning Robot",
    "key_outcome_metrics": "Less cleaning labour, safer floors",
    "key_challenge_of_solution": (
        "Routine floor cleaning was absorbing staff time the member would rather spend on guests "
        "and revenue-facing work, with no straightforward way to test a better method."
    ),
    "key_approach_of_solution": (
        "The ACES team trialled a floor robot on normal shifts, then locked the roster once it held up."
    ),
    "key_outcome_of_solution": (
        "The ACES team ran the trial without pulling staff off shift. Faster floor dry-down reduced "
        "slip exposure versus manual mop cycles."
    ),
    "key_outcome_dotpoints_1": "Less time on routine floor cleaning.",
    "key_outcome_dotpoints_2": "Annual labour saving at the agreed rate.",
    "key_outcome_dotpoints_3": "Faster dry floors and lower slip risk.",
    "key_outcome_dotpoints_4": "Less water for the same floor result.",
    "key_outcome_dotpoints_5": "A repeatable clean that can scale.",
    "conclusion": "A proven robot turns a daily manual clean into a documented annual saving.",
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 3, 6, 8, 9, 11, 12",
}

DEFAULT_CONTENT["solar_panel_cleaning"] = {
    "solution_type": "solar_panel_cleaning",
    "solution_type_label": "Solar Panel Cleaning",
    "key_outcome_metrics": "Higher yield after clean and inspection",
    "key_challenge_of_solution": (
        "Solar yield had drifted with soiling and minor defects, and faults were easy to miss."
    ),
    "key_approach_of_solution": (
        "The ACES team cleaned the array, completed an electrical inspection, and compared like-for-like generation."
    ),
    "key_outcome_of_solution": (
        "The ACES team measured the result off the member's own metering, so it is not a supplier's "
        "claim. Like-for-like intervals were used so the comparison could not be dismissed as a "
        "sunny-day spike."
    ),
    "key_outcome_dotpoints_1": "Higher daily generation after the clean.",
    "key_outcome_dotpoints_2": "Dollar value of the extra kWh.",
    "key_outcome_dotpoints_3": "Less grid energy and fewer emissions.",
    "key_outcome_dotpoints_4": "No electrical faults flagged.",
    "key_outcome_dotpoints_5": "Less risk of hidden panel degradation.",
    "conclusion": "A clean plus inspection restores yield and surfaces faults early.",
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 7, 8, 9, 13",
}

DEFAULT_CONTENT["client_endorsement"].update({
    "key_outcome_metrics": "One partner for energy and sustainability",
    "key_challenge_of_solution": (
        "The member was dealing with retailers, contractors and invoices in pieces, with no one "
        "owner who could explain what was worth doing and then actually do it."
    ),
    "key_approach_of_solution": (
        "The ACES team advised ongoing, ran the supplier conversations, and kept staff off the project work."
    ),
    "key_outcome_of_solution": (
        "The member had a named team they could call, a clear picture of what had been done, and "
        "less time spent chasing quotes, contracts and follow-up."
    ),
    "key_outcome_dotpoints_1": "One partner across energy and waste.",
    "key_outcome_dotpoints_2": "Supplier conversations handled for them.",
    "key_outcome_dotpoints_3": "Less time spent on bills and offers.",
    "key_outcome_dotpoints_4": "A record the board can read.",
    "key_outcome_dotpoints_5": "Support continues after the first saving.",
    "conclusion": "ACES as the member's energy team, not a one-off quote.",
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 8, 12, 17",
})

DEFAULT_CONTENT["association_endorsement"].update({
    "key_outcome_metrics": "Network outcomes and invoice support",
    "key_challenge_of_solution": (
        "Sub-branches were dealing with electricity and gas in isolation, with no shared "
        "programme and no one checking invoices after the contract was signed."
    ),
    "key_approach_of_solution": (
        "The ACES team ran group electricity and gas reviews, checked invoices, and stayed on after signing."
    ),
    "key_outcome_of_solution": (
        "Participating sub-branches secured rates, and the association had a partner still "
        "accountable after signing — not a one-off tender."
    ),
    "key_outcome_dotpoints_1": "Group electricity and gas reviews.",
    "key_outcome_dotpoints_2": "Invoices checked and errors resolved.",
    "key_outcome_dotpoints_3": "Support continued after signing.",
    "key_outcome_dotpoints_4": "Results visible across sub-branches.",
    "key_outcome_dotpoints_5": "One partner, not a one-off tender.",
    "conclusion": "A network reference from the peak body, not a single-site saving.",
    "esg_scope_for_solution": "SCOPE 2, 3",
    "sdg_impact_for_solution": "SDG 7, 12, 17",
})

DEFAULT_CONTENT["ghg_roadmap"].update({
    "key_outcome_metrics": "A board-ready greenhouse gas plan",
    "key_challenge_of_solution": (
        "The member needed a credible greenhouse-gas story for committees and stakeholders, but "
        "inventory, baselines and next actions were scattered or incomplete."
    ),
    "key_approach_of_solution": (
        "The ACES team built the roadmap from the member's own data and sequenced practical projects."
    ),
    "key_outcome_of_solution": (
        "The member received a plan they could defend: what the footprint is, which actions move it, "
        "and what can be done this year versus later."
    ),
    "key_outcome_dotpoints_1": "Scope 1, 2 and relevant Scope 3.",
    "key_outcome_dotpoints_2": "A sequenced plan, not a wish-list.",
    "key_outcome_dotpoints_3": "Actions tied to current operations.",
    "key_outcome_dotpoints_4": "Numbers a board can stand behind.",
    "key_outcome_dotpoints_5": "A baseline later projects report to.",
    "conclusion": "A greenhouse gas plan the member can update as projects land.",
    "esg_scope_for_solution": "SCOPE 1, 2, 3",
    "sdg_impact_for_solution": "SDG 7, 12, 13",
})

DEFAULT_CONTENT["solar_review"].update({
    "key_outcome_metrics": "Performance, tariff fit, and next steps",
    "key_challenge_of_solution": (
        "The member could not tell if the array was performing, or if an upgrade would pay back."
    ),
    "key_approach_of_solution": (
        "The ACES team checked generation, imports and the tariff against the member's actual load."
    ),
    "key_outcome_of_solution": (
        "The member had a clear finding: keep, maintain, or change the system and tariff, with numbers "
        "that could be shown to a committee."
    ),
    "key_outcome_dotpoints_1": "Generation checked against expected.",
    "key_outcome_dotpoints_2": "Tariff reviewed for the real load.",
    "key_outcome_dotpoints_3": "Upgrade claims tested, not assumed.",
    "key_outcome_dotpoints_4": "Cleaning flagged only if yield moves.",
    "key_outcome_dotpoints_5": "A recommendation ready for capex.",
    "conclusion": "A check of the solar asset, separate from a one-off panel clean.",
    "esg_scope_for_solution": "SCOPE 2",
    "sdg_impact_for_solution": "SDG 7, 9, 13",
})

DEFAULT_CONTENT["gas_discrepancy"].update({
    "key_outcome_metrics": "Recovered overcharge, corrected billing",
    "key_challenge_of_solution": (
        "Gas invoices did not match the contract or meter, and the member was absorbing the difference "
        "because reconciling retailer bills is slow and specialist."
    ),
    "key_approach_of_solution": (
        "The ACES team rebuilt the correct bill from the contract and meter, then claimed it."
    ),
    "key_outcome_of_solution": (
        "The overcharge was quantified and pursued, so the member was not left to argue a technical "
        "billing error without evidence."
    ),
    "key_outcome_dotpoints_1": "Invoice lines checked to the contract.",
    "key_outcome_dotpoints_2": "The dollar gap stated for finance.",
    "key_outcome_dotpoints_3": "ACES dealt with the retailer.",
    "key_outcome_dotpoints_4": "A paper trail for the credit.",
    "key_outcome_dotpoints_5": "Later bills watched for a repeat.",
    "conclusion": "Money back where the gas invoice did not match the agreement.",
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 12",
})

DEFAULT_CONTENT["electricity_discrepancy"].update({
    "key_outcome_metrics": "Corrected billing, recovered overcharge",
    "key_challenge_of_solution": (
        "Electricity invoices were out of step with the contract, meter or agreed adjustments, and "
        "the error was easy to miss in a long tax invoice."
    ),
    "key_approach_of_solution": (
        "The ACES team checked rates and quantities against the contract, then asked for a correction."
    ),
    "key_outcome_of_solution": (
        "The member had a quantified adjustment and a corrected billing position, without having to "
        "run the reconciliation in-house."
    ),
    "key_outcome_dotpoints_1": "Rates and quantities checked to the deal.",
    "key_outcome_dotpoints_2": "The dollar gap stated for finance.",
    "key_outcome_dotpoints_3": "Correction sent with the workings.",
    "key_outcome_dotpoints_4": "Network issues split from retailer error.",
    "key_outcome_dotpoints_5": "Later bills watched for a repeat.",
    "conclusion": "The electricity bill brought back in line with the agreement.",
    "esg_scope_for_solution": "SCOPE 2",
    "sdg_impact_for_solution": "SDG 7, 12",
})

DEFAULT_CONTENT["demand_reset"].update({
    "key_outcome_metrics": "Lower demand charges, ratchet reset",
    "key_challenge_of_solution": (
        "Network charges were still based on a high maximum demand that no longer reflected how the "
        "site ran, so the member was paying for a peak that was history."
    ),
    "key_approach_of_solution": (
        "The ACES team evidenced actual demand and ran the reset with the distributor."
    ),
    "key_outcome_of_solution": (
        "Demand was reset (or a clear pathway obtained) so ongoing network charges better matched "
        "current operations."
    ),
    "key_outcome_dotpoints_1": "Old peak compared with current demand.",
    "key_outcome_dotpoints_2": "Dollar impact of the reset.",
    "key_outcome_dotpoints_3": "Distributor process run by ACES.",
    "key_outcome_dotpoints_4": "Notes so a spike does not return.",
    "key_outcome_dotpoints_5": "The result shows on later invoices.",
    "conclusion": (
        "A demand reset is a network-charge outcome: stop paying for a peak the site no longer sets."
    ),
    "esg_scope_for_solution": "SCOPE 2",
    "sdg_impact_for_solution": "SDG 7, 12",
})

DEFAULT_CONTENT["cds"].update({
    "key_outcome_metrics": "Container refunds and less waste",
    "key_challenge_of_solution": (
        "Eligible drink containers were still going out with general waste or recycling, so refund "
        "value and diversion were being left on the table."
    ),
    "key_approach_of_solution": (
        "The ACES team set up a CDS path staff can run on shift, and a way to claim the refunds."
    ),
    "key_outcome_of_solution": (
        "Containers that qualify are separated and claimed, so the member sees both a small revenue "
        "line and a cleaner waste profile."
    ),
    "key_outcome_dotpoints_1": "Eligible containers picked out of waste.",
    "key_outcome_dotpoints_2": "A method staff can run on shift.",
    "key_outcome_dotpoints_3": "Refunds finance can see.",
    "key_outcome_dotpoints_4": "Less eligible stock in general waste.",
    "key_outcome_dotpoints_5": "Sits beside the waste review.",
    "conclusion": (
        "Refunds captured from containers that were previously thrown out."
    ),
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 12",
})


CONTENT_FIELD_KEYS = {
    "key_outcome_metrics",
    "key_challenge_of_solution",
    "key_approach_of_solution",
    "key_outcome_of_solution",
    "conclusion",
    "esg_scope_for_solution",
    "sdg_impact_for_solution",
    "key_outcome_dotpoints_1",
    "key_outcome_dotpoints_2",
    "key_outcome_dotpoints_3",
    "key_outcome_dotpoints_4",
    "key_outcome_dotpoints_5",
}


def slugify_solution_type(label: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", (label or "").strip().lower()).strip("_")
    if not text:
        raise ValueError("Solution type name must include at least one letter or number.")
    if not text[0].isalpha():
        text = f"type_{text}"
    return text[:80]


def _custom_row_to_content(row: Any) -> Dict[str, Any]:
    return {
        "solution_type": row.solution_type,
        "solution_type_label": row.solution_type_label,
        "key_outcome_metrics": row.key_outcome_metrics or "",
        "key_challenge_of_solution": row.key_challenge_of_solution or "",
        "key_approach_of_solution": row.key_approach_of_solution or "",
        "key_outcome_of_solution": row.key_outcome_of_solution or "",
        "key_outcome_dotpoints_1": row.key_outcome_dotpoints_1 or "",
        "key_outcome_dotpoints_2": row.key_outcome_dotpoints_2 or "",
        "key_outcome_dotpoints_3": row.key_outcome_dotpoints_3 or "",
        "key_outcome_dotpoints_4": row.key_outcome_dotpoints_4 or "",
        "key_outcome_dotpoints_5": row.key_outcome_dotpoints_5 or "",
        "conclusion": row.conclusion or "",
        "esg_scope_for_solution": row.esg_scope_for_solution or "",
        "sdg_impact_for_solution": row.sdg_impact_for_solution or "",
    }


def _get_custom_row(db: Any, solution_type_id: str) -> Any:
    from models import TestimonialSolutionType

    return (
        db.query(TestimonialSolutionType)
        .filter(TestimonialSolutionType.solution_type == solution_type_id)
        .first()
    )


def list_custom_contents(db: Any) -> List[Dict[str, Any]]:
    from models import TestimonialSolutionType

    rows = (
        db.query(TestimonialSolutionType)
        .order_by(TestimonialSolutionType.solution_type_label.asc())
        .all()
    )
    return [
        _custom_row_to_content(row)
        for row in rows
        if row.solution_type not in ALL_SOLUTION_TYPE_IDS
    ]


def create_custom_type(db: Any, label: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    from models import TestimonialSolutionType

    clean_label = (label or "").strip()
    if not clean_label:
        raise ValueError("Solution type name is required.")
    slug = slugify_solution_type(clean_label)
    if slug in ALL_SOLUTION_TYPE_IDS:
        existing_label = SOLUTION_TYPE_LABELS.get(slug, slug)
        raise ValueError(
            f"That name matches an existing type ({existing_label}). Edit that type instead."
        )
    existing = _get_custom_row(db, slug)
    if existing:
        n = 2
        candidate = f"{slug}_{n}"
        while candidate in ALL_SOLUTION_TYPE_IDS or _get_custom_row(db, candidate):
            n += 1
            candidate = f"{slug}_{n}"
        slug = candidate

    label_key = clean_label.casefold()
    for built_in_label in SOLUTION_TYPE_LABELS.values():
        if built_in_label.casefold() == label_key:
            raise ValueError(
                f"A type named {built_in_label!r} already exists. Edit that type instead."
            )
    for item in list_custom_contents(db):
        if (item.get("solution_type_label") or "").casefold() == label_key:
            raise ValueError(f"A type named {clean_label!r} already exists.")

    fields = {k: "" for k in CONTENT_FIELD_KEYS}
    fields.update(_default_content(slug))
    fields["solution_type"] = slug
    fields["solution_type_label"] = clean_label
    for key, value in (payload or {}).items():
        if key in CONTENT_FIELD_KEYS and value is not None:
            fields[key] = str(value).strip()

    row = TestimonialSolutionType(
        solution_type=slug,
        solution_type_label=clean_label,
        **{k: fields[k] for k in CONTENT_FIELD_KEYS},
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return _custom_row_to_content(row)


def delete_custom_type(db: Any, solution_type_id: str) -> None:
    """Remove a staff-created solution type. Built-in types cannot be deleted."""
    from models import TestimonialSolutionType

    clean_id = (solution_type_id or "").strip()
    if not clean_id:
        raise ValueError("solution_type is required.")
    if clean_id in ALL_SOLUTION_TYPE_IDS:
        raise ValueError("Built-in solution types cannot be deleted.")
    row = (
        db.query(TestimonialSolutionType)
        .filter(TestimonialSolutionType.solution_type == clean_id)
        .first()
    )
    if not row:
        raise LookupError(f"Unknown solution_type: {clean_id}")
    db.delete(row)
    db.commit()


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


def get_merged_content(solution_type_id: Optional[str] = None, db: Any = None) -> Any:
    """
    Return merged content (defaults + overrides + staff-created types).
    If solution_type_id is None, return list of merged content for all solution types.
    Otherwise return single merged dict for that solution type.
    """
    overrides = _load_overrides()
    if solution_type_id is not None:
        base = DEFAULT_CONTENT.get(solution_type_id)
        if base:
            merged = dict(base)
            if solution_type_id in overrides:
                for k, v in overrides[solution_type_id].items():
                    if v is not None and (isinstance(v, str) or not isinstance(merged.get(k), str)):
                        merged[k] = v
            return merged
        if db is not None:
            row = _get_custom_row(db, solution_type_id)
            if row:
                return _custom_row_to_content(row)
        return None
    result: List[Dict[str, Any]] = []
    for st in ALL_SOLUTION_TYPE_IDS:
        merged = get_merged_content(st, db)
        if merged:
            result.append(merged)
    if db is not None:
        result.extend(list_custom_contents(db))
    return result


def save_override(solution_type_id: str, payload: Dict[str, Any], db: Any = None) -> Dict[str, Any]:
    """
    Save override for one solution type. Built-in types go to the JSON file;
    staff-created types update the database row. Returns merged content after save.
    """
    if solution_type_id in ALL_SOLUTION_TYPE_IDS:
        overrides = _load_overrides()
        current = overrides.get(solution_type_id) or {}
        for k, v in payload.items():
            if k in CONTENT_FIELD_KEYS and v is not None:
                current[k] = str(v).strip() if isinstance(v, str) else v
        if "solution_type_label" in payload and payload["solution_type_label"] is not None:
            current["solution_type_label"] = str(payload["solution_type_label"]).strip()
        overrides[solution_type_id] = current
        _save_overrides(overrides)
        return get_merged_content(solution_type_id, db)

    if db is None:
        raise ValueError(f"Unknown solution_type: {solution_type_id}")
    row = _get_custom_row(db, solution_type_id)
    if not row:
        raise ValueError(f"Unknown solution_type: {solution_type_id}")
    for k, v in payload.items():
        if k in CONTENT_FIELD_KEYS and v is not None:
            setattr(row, k, str(v).strip() if isinstance(v, str) else v)
    label = payload.get("solution_type_label")
    if isinstance(label, str) and label.strip():
        row.solution_type_label = label.strip()
    db.add(row)
    db.commit()
    db.refresh(row)
    return _custom_row_to_content(row)
