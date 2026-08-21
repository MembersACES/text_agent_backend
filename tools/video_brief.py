"""Extract text from custom video source uploads."""

from __future__ import annotations

import io
import re
import zipfile
from typing import Optional
from xml.etree import ElementTree

_W_NS = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"


def slugify_title(title: str, fallback: str = "custom-video") -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (title or "").strip().lower()).strip("-")
    return slug[:80] or fallback


def generate_testimonial_slug(
    business_name: str,
    crm_solution_type_id: Optional[str] = None,
) -> str:
    """Deterministic slug for a testimonial not yet in the video registry."""
    name = slugify_title(business_name, "member")
    for suffix in ("-limited", "-ltd", "-pty-ltd", "-inc", "-incorporated"):
        if name.endswith(suffix):
            name = name[: -len(suffix)].rstrip("-")
    if crm_solution_type_id:
        st = crm_solution_type_id.strip().lower().replace("_", "-")
        if st and st not in name:
            return slugify_title(f"{name}-{st}", name)
    return name or "testimonial"


def extract_upload_text(content: bytes, filename: str, *, max_chars: int = 12000) -> str:
    """Best-effort plain text from common upload types."""
    name = (filename or "").lower()
    text = ""

    if name.endswith((".txt", ".md", ".markdown")):
        text = content.decode("utf-8", errors="replace")
    elif name.endswith(".docx"):
        text = _docx_text(content)
    elif name.endswith(".pdf"):
        text = _pdf_text(content)
    elif name.endswith(".doc"):
        text = content.decode("utf-8", errors="replace")

    text = re.sub(r"\s+", " ", (text or "").strip())
    if len(text) > max_chars:
        return text[: max_chars - 3] + "..."
    return text


def _docx_text(content: bytes) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            xml = zf.read("word/document.xml")
        root = ElementTree.fromstring(xml)
        parts = [el.text for el in root.iter(f"{_W_NS}t") if el.text]
        return " ".join(parts)
    except Exception:
        return ""


def _pdf_text(content: bytes) -> str:
    try:
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(content))
        parts = []
        for page in reader.pages:
            parts.append(page.extract_text() or "")
        return "\n".join(parts)
    except Exception:
        return ""
