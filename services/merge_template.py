"""Merge-template helpers. Keep in lockstep with text_agent_interface/src/lib/merge-template.ts."""

from __future__ import annotations

import re
from html import unescape
from html.parser import HTMLParser
from typing import Any

INTELLIGENCE_SENTINEL = "__intelligence__"

MERGE_FIELD_KEYS = (
    "first_name",
    "contact_name",
    "company_name",
    "contact_email",
    "contact_phone",
    "state",
    "city",
    "postcode",
    "industry",
)
MERGE_FIELD_KEY_SET = set(MERGE_FIELD_KEYS)

TOKEN_RE = re.compile(r"\{\{\s*([A-Za-z][A-Za-z0-9_]*)\s*\}\}")
ZERO_WIDTH_RE = re.compile(r"[\u200B-\u200D\uFEFF\u00AD]")


def normalize_template(template: str) -> str:
    return ZERO_WIDTH_RE.sub("", (template or "").replace("\xa0", " ").replace("&nbsp;", " "))


def extract_tokens(template: str) -> list[str]:
    return TOKEN_RE.findall(normalize_template(template))


def validate_template(template: str, allowed_keys: set[str]) -> list[str]:
    unknown: list[str] = []
    seen: set[str] = set()
    for token in extract_tokens(template):
        if token in allowed_keys or token in seen:
            continue
        seen.add(token)
        unknown.append(token)
    return unknown


def render_template(template: str, row: dict[str, str]) -> tuple[str, list[str]]:
    source = normalize_template(template)
    unresolved: list[str] = []
    seen: set[str] = set()

    def _repl(match: re.Match[str]) -> str:
        token = match.group(1)
        if token not in row:
            return match.group(0)
        value = row.get(token) or ""
        if value == "" and token not in seen:
            seen.add(token)
            unresolved.append(token)
        return value

    return TOKEN_RE.sub(_repl, source), unresolved


def html_to_plain_text(html: str) -> str:
    with_breaks = re.sub(r"<br\s*/?>", "\n", html or "", flags=re.I)
    with_breaks = re.sub(r"</(?:p|div|h[1-6]|li)>", "\n", with_breaks, flags=re.I)
    stripped = re.sub(r"<[^>]+>", "", with_breaks)
    stripped = unescape(stripped)
    stripped = stripped.replace("\xa0", " ")
    stripped = re.sub(r"[ \t]+\n", "\n", stripped)
    stripped = re.sub(r"\n{3,}", "\n\n", stripped)
    return stripped.strip()


def mapped_keys(column_map: dict[str, str]) -> set[str]:
    keys: set[str] = set()
    has_contact = False
    has_first = False
    for dest in column_map.values():
        if not dest or dest == INTELLIGENCE_SENTINEL:
            continue
        keys.add(dest)
        if dest == "contact_name":
            has_contact = True
        if dest == "first_name":
            has_first = True
    if has_contact and not has_first:
        keys.add("first_name")
    return keys


def split_row(
    headers: list[str],
    cells: list[str],
    column_map: dict[str, str],
) -> tuple[dict[str, str], dict[str, str]]:
    merge: dict[str, str] = {}
    intelligence: dict[str, str] = {}
    for i, header in enumerate(headers):
        raw = (cells[i] if i < len(cells) else "") or ""
        value = raw.strip()
        dest = column_map.get(header) or INTELLIGENCE_SENTINEL
        if dest == INTELLIGENCE_SENTINEL or dest not in MERGE_FIELD_KEY_SET:
            if value:
                intelligence[header] = value
            continue
        merge[dest] = value
    if "contact_name" in merge and "first_name" not in merge:
        merge["first_name"] = (merge["contact_name"].split() or [""])[0]
    return merge, intelligence


def recipient_key_from_merge(merge: dict[str, str]) -> str:
    return (merge.get("contact_email") or "").strip().lower()


class _Sanitizer(HTMLParser):
    ALLOWED = {
        "p", "br", "div", "span", "strong", "b", "em", "i", "u",
        "a", "ul", "ol", "li", "table", "thead", "tbody", "tr", "td", "th", "img",
    }
    VOID = {"br", "img"}
    ATTRS = {
        "a": {"href", "title"},
        "img": {"src", "alt", "width", "height"},
        "td": {"colspan", "rowspan"},
        "th": {"colspan", "rowspan"},
        "span": {"style"},
        "p": {"style"},
        "div": {"style"},
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.out: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag not in self.ALLOWED:
            return
        allowed = self.ATTRS.get(tag, set())
        parts = [tag]
        for name, value in attrs:
            if name.startswith("on"):
                continue
            if name not in allowed or value is None:
                continue
            if name in {"href", "src"} and not _safe_url(value):
                continue
            parts.append(f'{name}="{_escape_attr(value)}"')
        joined = " ".join(parts)
        if tag in self.VOID:
            self.out.append(f"<{joined}>")
        else:
            self.out.append(f"<{joined}>")

    def handle_endtag(self, tag: str) -> None:
        if tag not in self.ALLOWED or tag in self.VOID:
            return
        self.out.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        self.out.append(data.replace("<", "&lt;").replace(">", "&gt;"))


def _safe_url(value: str) -> bool:
    lowered = value.strip().lower()
    if lowered.startswith(("javascript:", "data:", "vbscript:")):
        return False
    return True


def _escape_attr(value: str) -> str:
    return value.replace("&", "&amp;").replace('"', "&quot;").replace("<", "&lt;")


def sanitize_html(html: str) -> str:
    parser = _Sanitizer()
    parser.feed(html or "")
    parser.close()
    return "".join(parser.out)


def looks_like_email(value: str) -> bool:
    text = (value or "").strip()
    if not text or any(sep in text for sep in (",", ";", " ")):
        return False
    return "@" in text and "." in text.split("@")[-1]


def parse_json_obj(raw: Any, default: dict | None = None) -> dict:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return default if default is not None else {}
    import json
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return default if default is not None else {}
    return data if isinstance(data, dict) else (default if default is not None else {})
