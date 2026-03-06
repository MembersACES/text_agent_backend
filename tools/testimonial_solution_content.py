"""
Testimonial solution content: defaults in code, overridable via JSON file.
Used to populate testimonial document templates (challenge, approach, outcome, dot points, etc.).
"""

import os
import json
import logging
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

# Optional extra solution types (e.g. case studies like Automated Cleaning Robot)
EXTRA_SOLUTION_TYPES: Dict[str, str] = {
    "automated_cleaning_robot": "Automated Cleaning Robot",
}

ALL_SOLUTION_TYPE_IDS = SOLUTION_TYPE_IDS + list(EXTRA_SOLUTION_TYPES.keys())
for k, v in EXTRA_SOLUTION_TYPES.items():
    SOLUTION_TYPE_LABELS[k] = v


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

# Example content for C&I Electricity (sensible copy)
DEFAULT_CONTENT["ci_electricity"].update({
    "key_outcome_metrics": "Energy Cost Reduction, Contract Optimisation",
    "key_challenge_of_solution": "High electricity costs and suboptimal retail contract terms for C&I sites.",
    "key_approach_of_solution": "Review of current contract, usage patterns and market rates; negotiation of improved rates and terms.",
    "key_outcome_of_solution": "Reduced monthly electricity spend with clearer contract terms and ongoing visibility.",
    "key_outcome_dotpoints_1": "Lower rates and improved contract structure.",
    "key_outcome_dotpoints_2": "Clearer billing and demand management visibility.",
    "key_outcome_dotpoints_3": "Ongoing monitoring and support.",
    "key_outcome_dotpoints_4": "",
    "key_outcome_dotpoints_5": "",
    "conclusion": "The C&I Electricity review delivered measurable savings and a more transparent contract.",
    "esg_scope_for_solution": "SCOPE 2",
    "sdg_impact_for_solution": "SDG 7, 12",
})

# Example for Automated Cleaning Robot (from boss's example)
DEFAULT_CONTENT["automated_cleaning_robot"] = {
    "solution_type": "automated_cleaning_robot",
    "solution_type_label": "Automated Cleaning Robot",
    "key_outcome_metrics": "Labour Optimisation, Cost Savings and Increased Cleaning",
    "key_challenge_of_solution": "Repetitive, time-consuming floor-cleaning tasks requiring significant manual labour for routine cleaning operations.",
    "key_approach_of_solution": "IGA Creswick deployed an autonomous cleaning robot to assist and increase weekly floor-cleaning. A trial of one vacuum and mopping bot was initiated to determine effectiveness over conventional methods.",
    "key_outcome_of_solution": "The robot performed extremely well and has been deployed full time, reducing cleaners by one day per week and freeing staff to focus on higher-value retail and customer engagement.",
    "key_outcome_dotpoints_1": "Labour Savings: 547.5 hours reduced per year for one robot.",
    "key_outcome_dotpoints_2": "Additional Cleaning: Store went from 3 cleans per week to 7 cleans per week, 5 by robot cleaner.",
    "key_outcome_dotpoints_3": "Cost Savings: Estimated annual reduction of $27,600 with a net outcome of $16,800 after robot rental costs.",
    "key_outcome_dotpoints_4": "Safety: Robot dries the floor almost instantly, reducing slip hazards.",
    "key_outcome_dotpoints_5": "Water Reduction: Significant reduction in water used for floor cleaning while achieving better results.",
    "conclusion": "A single automated cleaning robot delivers a measurable reduction in daily labour requirements and provides a repeatable annual operating cost saving. The cleaning robot is performing well.",
    "esg_scope_for_solution": "SCOPE 3",
    "sdg_impact_for_solution": "SDG 3, 6, 8, 9, 11, 12",
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
