"""Known product flows that can sit behind an autonomous sequence template."""
from __future__ import annotations

from typing import Any

# Wired starters in the UI, plus likely next comparisons that still need a sequence.
SEQUENCE_FLOW_CATALOG: list[dict[str, str]] = [
    {
        "sequence_type": "gas_base2_followup_v1",
        "display_name": "Gas Base 2 Follow-up",
        "source": "Base 2 — C&I Gas comparison",
        "copy_hint": "gas_base2_followup_v1",
    },
    {
        "sequence_type": "ci_electricity_base2_followup_v1",
        "display_name": "C&I Electricity Base 2 Follow-up",
        "source": "Base 2 — C&I Electricity comparison",
        "copy_hint": "ci_electricity_base2_followup_v1",
    },
    {
        "sequence_type": "ci_electricity_offer",
        "display_name": "C&I Electricity Offer",
        "source": "Utility Invoice Info — C&I Electricity comparison",
        "copy_hint": "ci_electricity_base2_followup_v1",
    },
    {
        "sequence_type": "bne_gas_base2_followup_v1",
        "display_name": "B&E Gas Base 2 Follow-up",
        "source": "Base 2 — B&E Gas comparison",
        "copy_hint": "gas_base2_followup_v1",
    },
    {
        "sequence_type": "sme_electricity_base2_followup_v1",
        "display_name": "SME Electricity Base 2 Follow-up",
        "source": "Base 2 — SME Electricity comparison",
        "copy_hint": "ci_electricity_base2_followup_v1",
    },
    {
        "sequence_type": "sme_gas_base2_followup_v1",
        "display_name": "SME Gas Base 2 Follow-up",
        "source": "Base 2 — SME Gas comparison",
        "copy_hint": "gas_base2_followup_v1",
    },
    {
        "sequence_type": "oil_base2_followup_v1",
        "display_name": "Oil Base 2 Follow-up",
        "source": "Base 2 — Oil comparison",
        "copy_hint": "gas_base2_followup_v1",
    },
    {
        "sequence_type": "waste_base2_followup_v1",
        "display_name": "Waste Base 2 Follow-up",
        "source": "Base 2 — Waste comparison",
        "copy_hint": "gas_base2_followup_v1",
    },
    {
        "sequence_type": "cleaning_base2_followup_v1",
        "display_name": "Cleaning Base 2 Follow-up",
        "source": "Base 2 — Cleaning comparison",
        "copy_hint": "gas_base2_followup_v1",
    },
    {
        "sequence_type": "ghg_offer_followup_v1",
        "display_name": "GHG Offer Follow-up",
        "source": "Document Generation — GHG offer",
        "copy_hint": "ci_electricity_offer",
    },
    {
        "sequence_type": "solar_panel_cleaning_followup_v1",
        "display_name": "Solar Panel Cleaning Follow-up",
        "source": "Solar cleaning quote sent",
        "copy_hint": "solar_panel_cleaning_followup_v1",
    },
    {
        "sequence_type": "solar_panel_cleaning_engagement_form_v1",
        "display_name": "Solar Panel Cleaning — Engagement Form",
        "source": "Document Generation — engagement form",
        "copy_hint": "solar_panel_cleaning_engagement_form_v1",
    },
]


def uncovered_flows(existing_sequence_types: set[str]) -> list[dict[str, Any]]:
    existing = {str(s).strip() for s in existing_sequence_types if str(s).strip()}
    out: list[dict[str, Any]] = []
    for row in SEQUENCE_FLOW_CATALOG:
        key = row["sequence_type"]
        if key in existing:
            continue
        hint = row.get("copy_hint") or ""
        out.append(
            {
                **row,
                "has_template": False,
                "copy_hint_available": hint in existing,
            }
        )
    return out
