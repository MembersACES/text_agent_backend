"""Shared scan-to-fields extraction: PDF text, embedded page images, rasterize, gpt-4o vision."""

from __future__ import annotations

import base64
import io
import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

SCAN_IMAGE_MIN_BYTES = 80_000
SCAN_MAX_PAGES = 6
VISION_MAX_EDGE = 2048
VISION_TIMEOUT_SECONDS = 90.0
OPENAI_VISION_MODEL = "gpt-4o"


def pdf_to_text(pdf_bytes: bytes) -> str:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(pdf_bytes))
    parts: list[str] = []
    for page in reader.pages:
        t = page.extract_text()
        if t:
            parts.append(t)
    return "\n".join(parts)


def pdf_embedded_page_images(pdf_bytes: bytes, max_pages: int = SCAN_MAX_PAGES) -> list[bytes]:
    """Full-page scan JPEGs/PNGs embedded in the PDF (typical of signed scans)."""
    from pypdf import PdfReader

    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
    except Exception:
        return []
    out: list[bytes] = []
    for page in reader.pages:
        try:
            images = list(page.images)
        except Exception:
            images = []
        for im in images:
            data = getattr(im, "data", None) or b""
            if len(data) >= SCAN_IMAGE_MIN_BYTES:
                out.append(data)
        if len(out) >= max_pages:
            break
    return out[:max_pages]


def rasterize_pdf_pages(pdf_bytes: bytes, max_pages: int = SCAN_MAX_PAGES) -> list[bytes]:
    """Render pages to JPEG when the PDF has no extractable image XObjects."""
    try:
        import pymupdf
    except ImportError:
        try:
            import fitz as pymupdf
        except ImportError:
            logger.warning("pymupdf not installed; cannot rasterize scanned PDF pages")
            return []

    try:
        doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        logger.warning("rasterize open failed: %s", e)
        return []

    out: list[bytes] = []
    try:
        page_count = min(len(doc), max_pages)
        matrix = pymupdf.Matrix(2, 2)
        for i in range(page_count):
            try:
                pix = doc.load_page(i).get_pixmap(matrix=matrix, alpha=False)
                png = pix.tobytes("png")
                out.append(jpeg_for_vision(png))
            except Exception as e:
                logger.warning("rasterize page %s failed: %s", i, e)
    finally:
        doc.close()
    return out


def collect_page_images(pdf_bytes: bytes, max_pages: int = SCAN_MAX_PAGES) -> list[bytes]:
    """Prefer a full-page raster. Embedded JPEGs are often a washed-out scan layer
    underneath vector/form text, which vision cannot read."""
    raster = rasterize_pdf_pages(pdf_bytes, max_pages=max_pages)
    if raster:
        return raster
    return pdf_embedded_page_images(pdf_bytes, max_pages=max_pages)


def jpeg_for_vision(image_bytes: bytes) -> bytes:
    from PIL import Image

    im = Image.open(io.BytesIO(image_bytes))
    if im.mode != "RGB":
        im = im.convert("RGB")
    width, height = im.size
    longest = max(width, height)
    if longest > VISION_MAX_EDGE:
        scale = VISION_MAX_EDGE / longest
        im = im.resize((int(width * scale), int(height * scale)), Image.Resampling.LANCZOS)
    buf = io.BytesIO()
    im.save(buf, format="JPEG", quality=90, optimize=True)
    return buf.getvalue()


def vision_extract_fields(
    images: list[bytes],
    prompt: str,
    keys: tuple[str, ...] | list[str],
    *,
    system: str = "You extract structured fields from scanned contracts. JSON only.",
) -> dict[str, str]:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key or not images:
        return {}
    import httpx

    parts: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
    for raw in images:
        try:
            jpeg = jpeg_for_vision(raw)
        except Exception as e:
            logger.warning("scan image could not be prepared: %s", e)
            continue
        b64 = base64.b64encode(jpeg).decode("ascii")
        parts.append(
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{b64}", "detail": "high"},
            }
        )
    if len(parts) < 2:
        return {}
    try:
        response = httpx.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENAI_VISION_MODEL,
                "temperature": 0,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": parts},
                ],
            },
            timeout=VISION_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        parsed = json.loads(content)
    except Exception as e:
        logger.warning("scan vision extract failed: %s", e)
        return {}
    if not isinstance(parsed, dict):
        return {}
    out: dict[str, str] = {}
    for key in keys:
        value = parsed.get(key)
        out[key] = "" if value is None else str(value).strip()
    return out
