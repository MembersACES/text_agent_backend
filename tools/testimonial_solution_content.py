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

# Defaults mirror frontend src/lib/testimonial-solution-content.ts
DEFAULT_CONTENT["ci_electricity"].update({
    "key_outcome_metrics": "Energy Cost Reduction, Contract Optimisation",
    "key_challenge_of_solution": "High electricity costs and suboptimal retail contract terms for C&I sites.",
    "key_approach_of_solution": (
        "Review of contract structure, usage data, and market pricing to identify savings opportunities "
        "and improve commercial terms."
    ),
    "key_outcome_of_solution": "Reduced energy costs with improved contract clarity and ongoing visibility.",
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
    "key_challenge_of_solution": "SME businesses often lack visibility into energy pricing and contract competitiveness.",
    "key_approach_of_solution": "Benchmarking current rates against market offers and simplifying contract structures.",
    "key_outcome_of_solution": "Lower electricity costs and simplified billing structures.",
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
        "Inconsistencies between contracted gas rates and invoiced charges, combined with limited market benchmarking."
    ),
    "key_approach_of_solution": "Forensic review of billing data and contracts alongside a market pricing review.",
    "key_outcome_of_solution": "Improved billing accuracy and more competitive forward gas pricing.",
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
    "key_challenge_of_solution": "SME gas customers often operate on unoptimised contracts with limited pricing visibility.",
    "key_approach_of_solution": "Market comparison and contract simplification.",
    "key_outcome_of_solution": "Reduced gas costs and improved billing clarity.",
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
        "Waste services are often misaligned with actual usage, leading to inefficiencies and unnecessary costs."
    ),
    "key_approach_of_solution": "Review of waste volumes, service frequency, and pricing structures.",
    "key_outcome_of_solution": "Optimised waste services and reduced operating costs.",
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
        "Used cooking oil is often treated only as waste with disposal costs, while biodiesel pathways "
        "can turn it into revenue. Sites may also overspend through high volumes of standard oil blends."
    ),
    "key_approach_of_solution": (
        "Combine resource recovery with a tighter cooking-oil procurement and usage model: benchmarking "
        "current vs optimised supply, aligning fry practices, and embedding monitoring so usage drops "
        "without hurting throughput or plate quality."
    ),
    "key_outcome_of_solution": (
        "Lower litres purchased, measurable kitchen efficiency gains, and a clearer link between fry "
        "performance, labour in the kitchen, and total oil spend."
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
    "key_challenge_of_solution": "Direct Metering Agreement charges were uncompetitive versus expected metering rates.",
    "key_approach_of_solution": "Forensic review of metering line items and the agreement against benchmarks.",
    "key_outcome_of_solution": "Lower metering costs quantified with a clear annual and multi-year saving.",
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
        "Routine floor cleaning absorbs repeated staff time and pulls people away from higher-value "
        "guest- or revenue-facing tasks."
    ),
    "key_approach_of_solution": (
        "Pilot an autonomous vacuum/mop robot on representative shifts, benchmark against incumbent "
        "methods (time, quality, rework), then standardise rostering once performance is proven."
    ),
    "key_outcome_of_solution": (
        "Freed labour hours redirected to core operations (e.g. service and member-facing work), "
        "alongside more consistent cleaning coverage and lower exposure to slips from wet floors."
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
        "Soiling and minor defects erode PV yield over time; many sites underservice cleaning while "
        "assuming periodic checks alone are sufficient."
    ),
    "key_approach_of_solution": (
        "Perform a systematic clean paired with at least a level 1 electrical inspection—then compare "
        "equivalent production intervals (aligned seasonality and metering) rather than anecdotal spikes."
    ),
    "key_outcome_of_solution": (
        "Restored yield on measured comparison intervals, translating to incremental kWh and an "
        "indicative emissions wedge where grid factors apply."
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
