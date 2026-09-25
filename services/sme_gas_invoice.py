"""Normalise SME gas invoice rows from the invoice sheet into one checked shape.

Rules agreed 25 Sep 2026 (project doc: sme-gas-invoice-normalisation-spec.md):

- Rates arrive as c/MJ incl GST. $/GJ = c/MJ x 10, done only in ``c_per_mj_to_aud_per_gj``.
- Usage is Blocks 1-4. General Usage Quantity is a fallback for old rows only, never added to blocks.
- Effective all-in rate = (block amounts + supply amount - discount $) / GJ.
  Discount columns hold separate credit lines only. "Plan Discount Included $" is already in
  the rates and is never subtracted again.
- Price change on invoice = Y: the row holds the latest price period only. Each MJ figure is
  paired with its own days (whole-bill MJ with invoice days, latest-period MJ with rates-period
  days). Block amounts are not reconciled against the invoice total on these rows.
- "Invoice Total:" is ex GST. Reconciliation uses "Invoice Total incl GST".
- Annual consumption of 1,000 GJ (1 TJ) or more is flagged as C&I.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any, Iterable, Optional

CI_THRESHOLD_GJ = 1000.0
NEAR_CI_THRESHOLD_GJ = 800.0
RECONCILE_TOLERANCE_AUD = 0.05

# MIRN prefix -> network. Longest prefix wins. The 531/532/533 split is the existing portal rule
# and is not always the real distributor (Leongatha is 531 and AusNet), so it stays "inferred".
_NETWORK_PREFIXES: list[tuple[str, dict[str, str]]] = [
    ("531", {"id": "multinet", "label": "Multinet", "state": "VIC"}),
    ("532", {"id": "agn_vic", "label": "Australian Gas Networks (VIC)", "state": "VIC"}),
    ("533", {"id": "ausnet", "label": "AusNet", "state": "VIC"}),
    ("55", {"id": "agn_sa", "label": "Australian Gas Networks (SA)", "state": "SA"}),
]
_STATE_PREFIXES = {"52": "NSW/ACT", "53": "VIC", "54": "QLD", "55": "SA", "56": "WA"}


def _key(name: str) -> str:
    # Keep $ and % distinct: "Usage Discount $" and "Usage Discount %" are different columns.
    text = str(name).lower().replace("$", " usd ").replace("%", " pct ")
    return re.sub(r"[^a-z0-9]", "", text)


class _Row:
    """Case, spacing and punctuation insensitive access to sheet columns."""

    def __init__(self, raw: dict[str, Any]):
        self.raw = raw
        self._by_key = {_key(k): v for k, v in raw.items()}

    def text(self, *names: str) -> str:
        for name in names:
            value = self._by_key.get(_key(name))
            if value is not None and str(value).strip() != "":
                return str(value).strip()
        return ""

    def num(self, *names: str) -> Optional[float]:
        return _num(self.text(*names))


def _num(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", "").replace("$", "").replace("%", "")
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def c_per_mj_to_aud_per_gj(c_per_mj: Optional[float]) -> Optional[float]:
    """The one place cents per MJ becomes dollars per GJ."""
    return None if c_per_mj is None else c_per_mj * 10.0


def _parse_dmy(text: str) -> Optional[date]:
    m = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*$", text or "")
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None


def parse_period(text: str) -> tuple[Optional[date], Optional[date]]:
    """'27/03/2026-24/06/2026' (with or without spaces) -> (start, end)."""
    parts = re.split(r"\s*(?:-|–|—|to)\s*", (text or "").strip(), maxsplit=1)
    if len(parts) != 2:
        return None, None
    start, end = _parse_dmy(parts[0]), _parse_dmy(parts[1])
    if not start or not end or start > end:
        return None, None
    return start, end


def parse_threshold(text: str) -> dict[str, Any]:
    """Printed block label -> upper bound and basis.

    Origin 'First 88767' / 'Next 88767 - 473424' are MJ per bill.
    EnergyAustralia '( 50.00000 MJ/day)' is MJ per day.
    'Remaining' is the open top block. Anything else is kept as unparsed.
    """
    label = (text or "").strip()
    out: dict[str, Any] = {"label": label, "upper_mj": None, "basis": None, "parsed": False}
    if not label:
        return out
    per_day = re.search(r"\(\s*([\d.,]+)\s*MJ\s*/\s*day\s*\)", label, re.I)
    if per_day:
        out.update(upper_mj=_num(per_day.group(1)), basis="per_day", parsed=True)
        return out
    first = re.match(r"^\s*first\s+([\d,.]+)\s*$", label, re.I)
    if first:
        out.update(upper_mj=_num(first.group(1)), basis="per_bill", parsed=True)
        return out
    nxt = re.match(r"^\s*next\s+[\d,.]+\s*-\s*([\d,.]+)\s*$", label, re.I)
    if nxt:
        out.update(upper_mj=_num(nxt.group(1)), basis="per_bill", parsed=True)
        return out
    if re.match(r"^\s*remaining\s*$", label, re.I):
        out.update(basis="remainder", parsed=True)
    return out


def network_from_mirn(mirn: str) -> dict[str, Any]:
    digits = re.sub(r"\D", "", mirn or "")
    for prefix, net in sorted(_NETWORK_PREFIXES, key=lambda p: -len(p[0])):
        if digits.startswith(prefix):
            return {**net, "match": "prefix"}
    return {"id": None, "label": None, "state": _STATE_PREFIXES.get(digits[:2]), "match": "none"}


def _flag(level: str, code: str, detail: str) -> dict[str, str]:
    return {"level": level, "code": code, "detail": detail}


def normalise_invoice(raw: dict[str, Any]) -> dict[str, Any]:
    row = _Row(raw)
    flags: list[dict[str, str]] = []

    mirn = re.sub(r"\D", "", row.text("MRIN", "MIRN"))
    rates_basis = row.text("Rates Basis")
    if rates_basis and _key(rates_basis) != _key("c/MJ incl GST"):
        flags.append(_flag("error", "rates_basis", f"Rates basis is '{rates_basis}', expected c/MJ incl GST. Not priced."))

    invoice_start, invoice_end = parse_period(row.text("Invoice Review Period"))
    rates_start, rates_end = parse_period(row.text("Rates Period"))
    invoice_days = row.num("Invoice Review Number of Days")
    rates_days = row.num("Rates Period Days") or invoice_days
    price_change = row.text("Price Change on Invoice").upper().startswith("Y")

    blocks = []
    for n in range(1, 5):
        qty = row.num(f"Block {n} Consumption")
        rate = row.num(f"Block {n} Rate")
        amount = row.num(f"Block {n} Amount $")
        if amount is None and qty is not None and rate is not None:
            amount = round(qty * rate / 100.0, 2)
        if qty is None and amount is None:
            continue
        blocks.append({
            "block": n,
            "mj": qty or 0.0,
            "rate_c_per_mj": rate,
            "rate_aud_per_gj": c_per_mj_to_aud_per_gj(rate),
            "amount_aud": amount or 0.0,
            "threshold": parse_threshold(row.text(f"Block {n} Threshold")),
        })

    for b in blocks:
        printed = row.num(f"Block {b['block']} Amount $")
        if printed is not None and b["mj"] and b["rate_c_per_mj"] is not None:
            implied = b["mj"] * b["rate_c_per_mj"] / 100.0
            if abs(implied - printed) > RECONCILE_TOLERANCE_AUD:
                flags.append(_flag("warning", "block_amount_mismatch",
                    f"Block {b['block']}: {b['mj']:,.0f} MJ x {b['rate_c_per_mj']} c/MJ = ${implied:,.2f}, "
                    f"but the invoice amount is ${printed:,.2f}. The rate was probably misread; the amount is used."))

    period_mj = sum(b["mj"] for b in blocks)
    usage_source = "blocks"
    if period_mj <= 0:
        general = row.num("General Usage Quantity")
        if general:
            period_mj = general
            usage_source = "general_usage_fallback"
            gen_rate = row.num("General Usage Rate")
            if gen_rate is not None:
                blocks = [{
                    "block": 1, "mj": general, "rate_c_per_mj": gen_rate,
                    "rate_aud_per_gj": c_per_mj_to_aud_per_gj(gen_rate),
                    "amount_aud": round(general * gen_rate / 100.0, 2),
                    "threshold": parse_threshold(""),
                }]
    if period_mj <= 0:
        flags.append(_flag("error", "no_usage", "No usage in Blocks 1-4 or General Usage Quantity."))

    supply_days = row.num("Daily Supply Charge Quantity") or rates_days
    supply_rate = row.num("Daily Supply Charge Rate")
    supply_amount = row.num("Daily Supply Amount $")
    if supply_amount is None and supply_rate is not None and supply_days:
        supply_amount = round(supply_rate * supply_days, 2)
    supply_amount = supply_amount or 0.0

    energy_amount = sum(b["amount_aud"] for b in blocks)

    usage_disc = row.num("Usage Discount $")
    supply_disc = row.num("Supply Discount $")
    total_disc = row.num("Total Discount $")
    if total_disc is None and (usage_disc is not None or supply_disc is not None):
        total_disc = (usage_disc or 0.0) + (supply_disc or 0.0)
    total_disc = abs(total_disc or 0.0)
    if usage_disc is None and supply_disc is None and total_disc:
        # Open decision: split a single discount line pro rata by amount (default).
        gross = energy_amount + supply_amount
        usage_disc = total_disc * (energy_amount / gross) if gross else total_disc
        supply_disc = total_disc - usage_disc
        flags.append(_flag("info", "discount_split_pro_rata",
                           f"${total_disc:,.2f} discount split pro rata between energy and supply."))
    usage_disc, supply_disc = abs(usage_disc or 0.0), abs(supply_disc or 0.0)

    for pct_col in ("Usage Discount %", "Supply Discount %", "Total Discount %"):
        pct = row.num(pct_col)
        dollars = row.num(pct_col.replace("%", "$"))
        if pct and not dollars:
            flags.append(_flag("info", "discount_pct_only",
                               f"{pct_col.strip()} is {pct:g} with no $ line. Not subtracted, since it may already be in the rates."))
            break
    other = row.text("Other Charges/Credits")
    if other:
        flags.append(_flag("info", "other_charges_note", other))

    gj = period_mj / 1000.0
    energy_net = energy_amount - usage_disc
    supply_net = supply_amount - supply_disc
    all_in = energy_amount + supply_amount - total_disc

    total_ex = row.num("Invoice Total")
    total_inc = row.num("Invoice Total incl GST")
    if price_change:
        reconcile = {"status": "not_checked_price_change", "calc_aud": round(all_in, 2), "invoice_incl_gst_aud": total_inc, "gap_aud": None}
    elif total_inc is None:
        reconcile = {"status": "no_total", "calc_aud": round(all_in, 2), "invoice_incl_gst_aud": None, "gap_aud": None}
    else:
        gap = round(total_inc - all_in, 2)
        ok = abs(gap) <= RECONCILE_TOLERANCE_AUD
        reconcile = {"status": "ok" if ok else "mismatch", "calc_aud": round(all_in, 2), "invoice_incl_gst_aud": total_inc, "gap_aud": gap}
        if not ok:
            flags.append(_flag("warning", "does_not_reconcile",
                               f"Line items ${all_in:,.2f} vs invoice incl GST ${total_inc:,.2f} (gap ${gap:,.2f})."))

    whole_bill_mj = row.num("Invoice Total MJ (whole bill)", "Invoice Total MJ")
    if price_change and whole_bill_mj:
        annual_basis = {"mj": whole_bill_mj, "days": invoice_days, "source": "whole_bill_mj_over_invoice_days"}
    elif price_change:
        annual_basis = {"mj": period_mj, "days": rates_days, "source": "latest_period_mj_over_rates_days"}
        flags.append(_flag("warning", "price_change_no_whole_bill_mj",
                           "Price change on invoice and no whole-bill MJ. Annual figure uses the latest price period only."))
    else:
        annual_basis = {"mj": period_mj, "days": invoice_days, "source": "period_mj_over_invoice_days"}

    annual_gj = None
    if annual_basis["mj"] and annual_basis["days"]:
        annual_gj = annual_basis["mj"] / 1000.0 * 365.0 / annual_basis["days"]

    network = network_from_mirn(mirn)
    if network["id"] is None:
        flags.append(_flag("error", "no_network", f"No gas network mapped for MIRN prefix {mirn[:3] or '?'}."))

    return {
        "mirn": mirn,
        "client_name": row.text("Client Name"),
        "retailer": row.text("Retailer"),
        "plan_name": row.text("Plan Name"),
        "invoice_number": row.text("Invoice Number"),
        "webview_link": row.text("Webview Link"),
        "supply_address": row.text("Supply Address"),
        "network": network,
        "invoice_period": {"start": _iso(invoice_start), "end": _iso(invoice_end), "days": invoice_days},
        "rates_period": {"start": _iso(rates_start), "end": _iso(rates_end), "days": rates_days},
        "price_change_on_invoice": price_change,
        "usage_source": usage_source,
        "period_mj": period_mj,
        "period_gj": gj,
        "blocks": blocks,
        "supply": {"days": supply_days, "rate_aud_per_day": supply_rate, "amount_aud": supply_amount},
        "discounts": {"usage_aud": usage_disc, "supply_aud": supply_disc, "total_aud": total_disc,
                      "plan_discount_included_aud": row.num("Plan Discount Included $")},
        "energy_rate_aud_per_gj": energy_net / gj if gj else None,
        "supply_aud_per_day": supply_net / supply_days if supply_days else None,
        "all_in_aud_per_gj": all_in / gj if gj else None,
        "invoice_total_ex_gst_aud": total_ex,
        "invoice_total_incl_gst_aud": total_inc,
        "gst_aud": row.num("GST $"),
        "reconcile": reconcile,
        "annual_basis": annual_basis,
        "annual_gj_this_invoice": annual_gj,
        "flags": flags,
    }


def _iso(d: Optional[date]) -> Optional[str]:
    return d.isoformat() if d else None


def classify(annual_gj: Optional[float]) -> dict[str, Any]:
    if annual_gj is None:
        return {"class": None, "flag": None}
    if annual_gj >= CI_THRESHOLD_GJ:
        return {"class": "ci", "flag": _flag("warning", "ci_volume",
                f"{annual_gj:,.0f} GJ/yr is 1 TJ or more. Price as C&I, not SME vs SME.")}
    if annual_gj >= NEAR_CI_THRESHOLD_GJ:
        return {"class": "sme", "flag": _flag("info", "near_ci_volume",
                f"{annual_gj:,.0f} GJ/yr is close to the 1 TJ C&I threshold.")}
    return {"class": "sme", "flag": None}


def summarise_mirn(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Combine every invoice for one MIRN. Dedupes by invoice number; annual = sum MJ / sum days x 365."""
    seen: dict[str, dict[str, Any]] = {}
    for raw in rows:
        inv = normalise_invoice(raw)
        key = inv["invoice_number"] or f"{inv['invoice_period']['start']}|{inv['invoice_period']['end']}"
        seen.setdefault(key, inv)
    invoices = sorted(seen.values(), key=lambda i: i["invoice_period"]["start"] or "")
    if not invoices:
        return {"mirn": None, "invoices": [], "annual_gj": None, "flags": []}

    usable = [i for i in invoices if i["annual_basis"]["mj"] and i["annual_basis"]["days"]]
    sum_mj = sum(i["annual_basis"]["mj"] for i in usable)
    sum_days = sum(i["annual_basis"]["days"] for i in usable)
    annual_gj = sum_mj / 1000.0 * 365.0 / sum_days if sum_days else None

    gaps = []
    for prev, nxt in zip(invoices, invoices[1:]):
        pe, ns = prev["invoice_period"]["end"], nxt["invoice_period"]["start"]
        if pe and ns and date.fromisoformat(ns) > date.fromisoformat(pe) + timedelta(days=1):
            gaps.append({"from": (date.fromisoformat(pe) + timedelta(days=1)).isoformat(),
                         "to": (date.fromisoformat(ns) - timedelta(days=1)).isoformat()})

    flags: list[dict[str, str]] = []
    cls = classify(annual_gj)
    if cls["flag"]:
        flags.append(cls["flag"])
    if gaps:
        flags.append(_flag("info", "coverage_gaps", f"{len(gaps)} gap(s) between invoices. Annual figure is from covered days only."))
    if sum_days and sum_days < 300:
        flags.append(_flag("info", "part_year", f"Annual figure is scaled up from {sum_days:.0f} days, so seasonal mix may be off."))

    latest = invoices[-1]
    return {
        "mirn": latest["mirn"],
        "network": latest["network"],
        "latest": latest,
        "invoices": invoices,
        "invoice_count": len(invoices),
        "coverage_days": sum_days,
        "gaps": gaps,
        "annual_gj": annual_gj,
        "class": cls["class"],
        "flags": flags,
    }
