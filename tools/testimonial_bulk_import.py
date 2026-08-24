"""Map colleague testimonial zip entries to solution types and CRM members.

Used by scripts/bulk_upload_testimonials.py. Pure helpers — no HTTP.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable, Sequence

from tools.testimonial_solution_content import SOLUTION_TYPE_LABELS


ALLOWED_EXTENSIONS = {".pdf", ".docx", ".doc", ".png", ".jpg", ".jpeg"}

SKIP_TOP_FOLDERS = frozenset({"offers", "new layout"})
SKIP_PATH_PARTS = frozenset({"story format", "already moved", "amended ones"})

FOLDER_TO_TYPE: dict[str, str] = {
    "electricity contract": "ci_electricity",
    "gas contract": "ci_gas",
    "waste": "waste",
    "cooking oil": "resource_recovery",
    "dma": "dma",
    "cleaning robot": "automated_cleaning_robot",
    "solar cleaning": "solar_panel_cleaning",
    "service": "client_endorsement",
    "ghg": "ghg_roadmap",
    "solar review": "solar_review",
    "gas discrepancy": "gas_discrepancy",
    "electricity discrepancy": "electricity_discrepancy",
    "demand reset": "demand_reset",
    "cds": "cds",
}

# Longest suffix first so "GHG Roadmap and Cooking Oil" does not become cooking oil.
AAA_SUFFIX_TO_TYPE: tuple[tuple[str, str], ...] = (
    ("ghg roadmap and cooking oil", "ghg_roadmap"),
    ("gas billing discrepancy recovery", "gas_discrepancy"),
    ("gas discrepancy recovery", "gas_discrepancy"),
    ("sustainable waste review", "waste"),
    ("direct metering agreement", "dma"),
    ("gas contract review", "ci_gas"),
    ("solar panel cleaning", "solar_panel_cleaning"),
    ("cooking oil review", "resource_recovery"),
    ("client endorsement", "client_endorsement"),
    ("solar review", "solar_review"),
)

_TYPE_PHRASES = tuple(
    sorted(
        {
            "automated cleaning robot testimonial",
            "automated cleaning robot",
            "direct metering agreement",
            "sustainable waste review",
            "gas billing discrepancy recovery",
            "gas discrepancy recovery",
            "gas contract review",
            "solar panel cleaning",
            "cooking oil review",
            "client endorsement",
            "ghg roadmap and cooking oil",
            "ghg roadmap",
            "electricity meter",
            "max demand reset",
            "discrepancy adjust",
            "waste review",
            "cooking oil",
            "solar cleaning",
            "solar review",
            "testimonial",
            "testiomonial",
            "google docs",
            "revew",
            "result",
            "minimalist",
            "e-c&i",
            "g-c&i",
            "cds",
            "dma",
            "ghg",
            "gas",
            "waste",
        },
        key=len,
        reverse=True,
    )
)

_STOPWORDS = frozenset(
    {
        "pty",
        "ltd",
        "inc",
        "incorporated",
        "sub",
        "branch",
        "the",
        "and",
        "copy",
        "of",
        "for",
        "google",
        "docs",
        "review",
        "revew",
        "testiomonial",
        "resort",
        "citizens",
        "club",
    }
)

_HINT_ALIASES: dict[str, tuple[str, ...]] = {
    "richmond fc": ("richmond football", "richmond fc", "richmond"),
    "swin alumni": ("swin alumni", "geelong surfcoast laundry", "surfcoast laundry"),
    "geelong surfcoast laundry": ("swin alumni", "geelong surfcoast laundry"),
    "licola jmc ta esplanade hotel": ("esplanade hotel", "licola"),
    "masonic": ("masonic club", "masonic"),
    "north melbourne": ("north melbourne",),
    "rose lodge": ("rose lodge",),
    "coolan nominees": ("coolan",),
    "rich river golf": ("rich river",),
    "gosford sailing": ("gosford sailing",),
}


class SkipReason(str, Enum):
    DIRECTORY = "directory"
    WORD_LOCK = "word_lock"
    UNSUPPORTED_EXTENSION = "unsupported_extension"
    NOT_TESTIMONIAL_FOLDER = "not_a_testimonial_folder"
    DUPLICATE_LAYOUT = "duplicate_or_story_layout"
    ANALYSIS_OR_PRESENTATION = "analysis_or_presentation"
    UNMAPPED_FOLDER = "unmapped_folder"
    UNMAPPED_AAA_SUFFIX = "unmapped_aaa_suffix"
    NO_MEMBER_HINT = "no_member_hint"
    DUPLICATE_OF_PREFERRED = "duplicate_of_preferred"


@dataclass(frozen=True)
class ClassifiedEntry:
    zip_path: str
    filename: str
    solution_type_id: str | None
    member_hint: str
    skip_reason: SkipReason | None
    preferred: bool = False

    @property
    def kept(self) -> bool:
        return self.skip_reason is None and bool(self.solution_type_id)


@dataclass(frozen=True)
class CrmClient:
    business_name: str
    gdrive_folder_url: str | None = None


@dataclass(frozen=True)
class MemberMatch:
    business_name: str
    gdrive_folder_url: str | None
    score: int
    ambiguous: bool


def normalize_folder(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip().lower())


def normalize_person_name(name: str) -> str:
    text = (name or "").lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = [t for t in text.split() if t and t not in _STOPWORDS and not t.isdigit()]
    return " ".join(tokens)


def solution_type_label(solution_type_id: str) -> str:
    return SOLUTION_TYPE_LABELS.get(solution_type_id, solution_type_id.replace("_", " ").title())


def member_hint_from_filename(filename: str) -> str:
    stem = Path(filename).stem.replace("_", " ")
    text = re.sub(r"\s+", " ", stem).strip()
    text = re.sub(r"(?i)^copy of\s+", "", text)
    text = re.sub(r"(?i)^testimonial\s*[-–_:]+\s*", "", text)
    text = re.sub(r"(?i)step\s*1\s*\([^)]*\)", " ", text)
    text = re.sub(r"\(\d+\s*[-/]\s*\d+\)", " ", text)
    text = re.sub(r"\(\d+\)", " ", text)
    text = re.sub(r"\b\d{1,2}\.\d{2}(?:\.\d{2,4})?\b", " ", text)
    text = re.sub(r"\b20\d{2}(?:\.\d{1,2}){0,2}\b", " ", text)
    lowered = text.lower()
    for phrase in _TYPE_PHRASES:
        lowered = re.sub(re.escape(phrase), " ", lowered, flags=re.IGNORECASE)
    lowered = re.sub(r"[-–:&]+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip(" ._-")
    # Restore a readable title-ish hint from the stripped lowercase string
    # by taking leftover tokens from the original where possible.
    leftover_tokens = lowered.split()
    if not leftover_tokens:
        return ""
    original_tokens = re.sub(r"\s+", " ", text).split()
    kept: list[str] = []
    used = 0
    for token in original_tokens:
        key = re.sub(r"[^a-z0-9]", "", token.lower())
        if used < len(leftover_tokens) and leftover_tokens[used] == key:
            kept.append(token)
            used += 1
        elif key and leftover_tokens[used] in key:
            kept.append(token)
            used += 1
        if used >= len(leftover_tokens):
            break
    if len(kept) >= 2 or (kept and len(kept[0]) > 3):
        return " ".join(kept)
    return " ".join(w.title() if w.islower() else w for w in leftover_tokens)


def type_from_aaa_filename(filename: str) -> str | None:
    stem = Path(filename).stem.replace("_", " ").lower()
    stem = re.sub(r"\s+", " ", stem)
    for suffix, type_id in AAA_SUFFIX_TO_TYPE:
        if stem.endswith(suffix) or f" - {suffix}" in stem:
            return type_id
    return None


def _skip(zip_path: str, filename: str, reason: SkipReason) -> ClassifiedEntry:
    return ClassifiedEntry(
        zip_path=zip_path,
        filename=filename,
        solution_type_id=None,
        member_hint="",
        skip_reason=reason,
    )


def classify_zip_path(zip_path: str) -> ClassifiedEntry:
    posix = zip_path.replace("\\", "/")
    if posix.endswith("/"):
        return _skip(posix, "", SkipReason.DIRECTORY)
    parts = [p for p in posix.split("/") if p]
    filename = parts[-1]
    if filename.startswith("~$"):
        return _skip(posix, filename, SkipReason.WORD_LOCK)
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        return _skip(posix, filename, SkipReason.UNSUPPORTED_EXTENSION)

    lower_name = filename.lower()
    if "analysis" in lower_name or "presentation" in lower_name:
        return _skip(posix, filename, SkipReason.ANALYSIS_OR_PRESENTATION)

    folder_parts = [normalize_folder(p) for p in parts[:-1]]
    top = folder_parts[0] if folder_parts else ""
    if not top or top in SKIP_TOP_FOLDERS:
        return _skip(posix, filename, SkipReason.NOT_TESTIMONIAL_FOLDER)
    if any(part in SKIP_PATH_PARTS for part in folder_parts):
        return _skip(posix, filename, SkipReason.DUPLICATE_LAYOUT)

    type_id: str | None
    if top.startswith("aaa"):
        type_id = type_from_aaa_filename(filename)
        if not type_id:
            return _skip(posix, filename, SkipReason.UNMAPPED_AAA_SUFFIX)
    else:
        type_id = FOLDER_TO_TYPE.get(top)
        if not type_id:
            return _skip(posix, filename, SkipReason.UNMAPPED_FOLDER)

    hint = member_hint_from_filename(filename)
    if not hint:
        return _skip(posix, filename, SkipReason.NO_MEMBER_HINT)
    return ClassifiedEntry(
        zip_path=posix,
        filename=filename,
        solution_type_id=type_id,
        member_hint=hint,
        skip_reason=None,
    )


def _preference_score(entry: ClassifiedEntry) -> tuple[int, int, str]:
    path_l = entry.zip_path.lower()
    name_l = entry.filename.lower()
    score = 0
    if path_l.startswith("aaa") and "story format" not in path_l:
        score += 100
    ext = Path(entry.filename).suffix.lower()
    if ext in {".docx", ".doc"}:
        score += 50
    elif ext == ".pdf":
        score += 40
    elif ext in {".png", ".jpg", ".jpeg"}:
        score += 10
    if name_l.startswith("copy of"):
        score -= 20
    if "analysis" in name_l:
        score -= 80
    if re.search(r"\(\d+\)", name_l):
        score -= 2
    # Prefer the shorter stem when two scans differ only by trailing space.
    return (score, -len(entry.filename), entry.zip_path)


def plan_zip_entries(zip_paths: Iterable[str]) -> list[ClassifiedEntry]:
    classified = [classify_zip_path(p) for p in zip_paths]
    groups: dict[tuple[str, str], list[int]] = {}
    for idx, entry in enumerate(classified):
        if not entry.kept or not entry.solution_type_id:
            continue
        key = (normalize_person_name(entry.member_hint), entry.solution_type_id)
        groups.setdefault(key, []).append(idx)

    preferred: set[int] = set()
    for indexes in groups.values():
        winner = max(indexes, key=lambda i: _preference_score(classified[i]))
        preferred.add(winner)

    out: list[ClassifiedEntry] = []
    for idx, entry in enumerate(classified):
        if not entry.kept:
            out.append(entry)
            continue
        if idx in preferred:
            out.append(
                ClassifiedEntry(
                    zip_path=entry.zip_path,
                    filename=entry.filename,
                    solution_type_id=entry.solution_type_id,
                    member_hint=entry.member_hint,
                    skip_reason=None,
                    preferred=True,
                )
            )
        else:
            out.append(
                ClassifiedEntry(
                    zip_path=entry.zip_path,
                    filename=entry.filename,
                    solution_type_id=entry.solution_type_id,
                    member_hint=entry.member_hint,
                    skip_reason=SkipReason.DUPLICATE_OF_PREFERRED,
                    preferred=False,
                )
            )
    return out


def _alias_needles(hint: str) -> tuple[str, ...]:
    key = normalize_person_name(hint)
    extra = _HINT_ALIASES.get(key, ())
    return (key, *extra)


def score_client(hint: str, business_name: str) -> int:
    needles = [n for n in _alias_needles(hint) if n]
    target = normalize_person_name(business_name)
    if not needles or not target:
        return 0
    best = 0
    for needle in needles:
        if needle == target:
            best = max(best, 100)
            continue
        if needle in target or target in needle:
            best = max(best, 80 + min(len(needle), 15))
            continue
        nt = set(needle.split())
        tt = set(target.split())
        if not nt:
            continue
        overlap = nt & tt
        distinctive = overlap - {"rsl", "club", "golf", "hotel"}
        if not distinctive:
            continue
        ratio = int(70 * len(overlap) / len(nt))
        best = max(best, ratio)
    return best


def match_crm_member(hint: str, clients: Sequence[CrmClient]) -> MemberMatch | None:
    if not hint or not clients:
        return None
    scored = [(score_client(hint, c.business_name), c) for c in clients]
    scored.sort(key=lambda row: row[0], reverse=True)
    top_score, top = scored[0]
    if top_score < 55:
        return None
    tied = [c for s, c in scored if s == top_score]
    second = scored[1][0] if len(scored) > 1 else 0
    ambiguous = len(tied) > 1 or (second >= 55 and top_score - second < 8)
    if ambiguous:
        return MemberMatch(
            business_name=top.business_name,
            gdrive_folder_url=top.gdrive_folder_url,
            score=top_score,
            ambiguous=True,
        )
    return MemberMatch(
        business_name=top.business_name,
        gdrive_folder_url=top.gdrive_folder_url,
        score=top_score,
        ambiguous=False,
    )
