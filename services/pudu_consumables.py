from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

_INTERVAL_RE = re.compile(
    r"^\s*(\d+(?:\.\d+)?)\s*(month|months|year|years|day|days)\s*$",
    re.IGNORECASE,
)


ConsumableRow = Dict[str, Any]


def _row(
    mode_name: str,
    sku: str,
    name: str,
    quantity: Optional[float],
    unit: str,
    rrp: str,
    lifespan_per_unit: str,
    hours: Optional[float],
    item_type: str,
    notes: str = "",
) -> ConsumableRow:
    return {
        "mode_name": mode_name,
        "sku": sku,
        "name": name,
        "quantity": quantity,
        "unit": unit,
        "rrp": rrp,
        "lifespan_per_unit": lifespan_per_unit,
        "hours": hours,
        "item_type": item_type,
        "notes": notes,
    }


DEFAULT_CONSUMABLE_TEMPLATES: Dict[str, List[ConsumableRow]] = {
    "cc1": [
        _row("CC1 Scrubbing Mode", "19888-000054", "Scrubbing dust box assembly", 1, "pcs", "$110/each", "1 year", None, "Consumables"),
        _row("CC1 Scrubbing Mode", "19888-000052", "Air outlet filter cotton", 1, "pcs", "$15/10pcs", "3 months", None, "Consumables"),
        _row("CC1 Scrubbing Mode", "19999-000711", "Squeegee-rubber strip", 1, "pair", "$55/pair", "3 months", None, "Consumables"),
        _row("CC1 Scrubbing Mode", "19888-000043", "Scrubbing roller brush", 1, "pair", "$320/pair", "6 months", None, "Consumables"),
        _row("CC1 Scrubbing Mode", "", "Neutrol Floor Cleaner", 5, "Litre", "$30/5L", "600L of water", None, "Consumables"),
        _row("CC1 Scrubbing Mode", "19999-000707", "Squeegee-rubber assembly", 1, "pcs", "$465/pcs", "", None, "Parts", "Often found broken by cleaners"),
        _row("CC1 Scrubbing Mode", "19999-001198", "Drain Paip", 1, "pcs", "$70/pcs", "", None, "Parts", "Often found broken by cleaners due to mishandling"),
        _row("CC1 Vacuuming Mode", "19888-000044", "Sweeping roller brush (N)", 1, "pair", "$240/pair", "6 months", None, "Consumables"),
        _row("CC1 Vacuuming Mode", "19888-000014", "Rubber-coated side brush - anti-drop", 1, "pcs", "$33/pair", "6 months", None, "Consumables"),
        _row("CC1 Vacuuming Mode", "19888-000053", "Dustbox", 1, "pcs", "$110/each", "1 year", None, "Consumables"),
        _row("CC1 Vacuuming Mode", "19888-000052", "Air outlet filter cotton", 1, "pcs", "$15/10pcs", "3 months", None, "Consumables"),
        _row("CC1 Vacuuming Mode", "19999-001198", "Drain Paip", 1, "pcs", "$70/pcs", "", None, "Parts", "Often found broken by cleaners due to mishandling"),
    ],
    "mt1": [
        _row("MT1 / MT1 Max Sweeping Mode", "19888-000026", "MT1 sweep side brush(hard)", 1, "pair", "$200/pair", "6 months", None, "Consumables"),
        _row("MT1 / MT1 Max Sweeping Mode", "19888-000064", "Removable sweep rolling brush", 1, "pcs", "$240/pcs", "1 year", None, "Consumables"),
        _row("MT1 / MT1 Max Sweeping Mode", "19888-000028", "MT1 Air Filter", 1, "pcs", "$210/pair", "6 months", None, "Consumables"),
        _row("MT1 / MT1 Max Sweeping Mode", "19888-000030", "MT1 Waste box sealing rubber", 1, "pcs", "$35/2x pieces", "6 months", None, "Consumables"),
        _row("MT1 / MT1 Max Sweeping Mode", "19888-000031", "MT1 sweep rolling brush sealing rubber", 1, "pcs", "$35/2x pieces", "6 months", None, "Consumables"),
    ],
    "mt1vac": [
        _row("MT1 Vac Vacuuming + Sweeping Mode", "19888-000030", "MT1 Waste box sealing rubber", 1, "pcs", "$35/2x pieces", "6 months", None, "Consumables"),
        _row("MT1 Vac Vacuuming + Sweeping Mode", "19888-000031", "MT1 sweep rolling brush sealing rubber", 1, "pcs", "$35/2x pieces", "6 months", None, "Consumables"),
        _row("MT1 Vac Vacuuming + Sweeping Mode", "19888-000056", "MT1 Vac side brush (soft)", 1, "pair", "$330/pair", "6 months", None, "Consumables"),
        _row("MT1 Vac Vacuuming + Sweeping Mode", "19888-000069", "Detachable roller brush (antistatic)", 1, "pcs", "$465/pcs", "1 year", None, "Consumables"),
        _row("MT1 Vac Vacuuming + Sweeping Mode", "19888-000058", "MT1 Vac vacuum assembly rubber", 1, "pair", "$225/6x pairs", "3 months", None, "Consumables"),
        _row("MT1 Vac Vacuuming + Sweeping Mode", "19888-000059", "MT1 Vac Dustbag", 1, "pair", "$120/6x pairs", "3 months", None, "Consumables"),
        _row("MT1 Vac Vacuuming + Sweeping Mode", "19888-000061", "MT1 Vac HEPA", 1, "pair", "$70/6x pairs", "3 months", None, "Consumables"),
    ],
    "sh1": [
        _row("SH1", "19888-000003", "Blue Rubber Blades", 1, "pair", "$42/pair", "6 months", None, "Consumables"),
        _row("SH1", "19888-000005", "Black Disc Brush", 1, "pair", "$180/pair", "6 months", None, "Consumables"),
        _row("SH1", "19999-001993", "Squeegee Height Adjustment Wheel Kit", 3, "pcs", "$10/each", "3 months", None, "Consumables"),
        _row("SH1", "", "Neutrol Floor Cleaner", 5, "Litre", "$30/5L", "600L of water", None, "Consumables"),
    ],
}


def normalize_product_template_key(product_hint: str) -> Optional[str]:
    t = (product_hint or "").strip().upper().replace(" ", "")
    if not t:
        return None
    if "MT1VAC" in t:
        return "mt1vac"
    if "MT1" in t:
        return "mt1"
    if "SH1" in t:
        return "sh1"
    if "CC1" in t:
        return "cc1"
    return None


def default_consumables_for_product(product_hint: str) -> List[ConsumableRow]:
    key = normalize_product_template_key(product_hint)
    if not key:
        return []
    rows = DEFAULT_CONSUMABLE_TEMPLATES.get(key, [])
    return [dict(r) for r in rows]


def approx_replacement_interval_days(lifespan_per_unit: str) -> Optional[int]:
    """
    Best-effort parse of template strings like "6 months" or "1 year" into approximate calendar days.
    Returns None when the text is empty or not a simple N unit pattern (e.g. "600L of water").
    """
    m = _INTERVAL_RE.match((lifespan_per_unit or "").strip())
    if not m:
        return None
    n = float(m.group(1))
    unit = m.group(2).lower()
    if unit.startswith("year"):
        return max(1, int(round(n * 365)))
    if unit.startswith("month"):
        return max(1, int(round(n * 30)))
    if unit.startswith("day"):
        return max(1, int(round(n)))
    return None
