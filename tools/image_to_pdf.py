"""Convert PNG/JPEG uploads to a single-page A4 PDF so testimonials are never stored as images."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Tuple

from PIL import Image, ImageFile, UnidentifiedImageError
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas

ImageFile.LOAD_TRUNCATED_IMAGES = True

IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg")
MAX_IMAGE_EDGE_PX = 3500
PAGE_MARGIN_PT = 36


def is_image_filename(filename: str) -> bool:
    return Path(filename or "").suffix.lower() in IMAGE_EXTENSIONS


def pdf_filename_from_image(original_filename: str) -> str:
    stem = Path(original_filename or "testimonial").stem.strip() or "testimonial"
    for ch in '\\/:*?"<>|':
        stem = stem.replace(ch, "-")
    stem = " ".join(stem.split())
    return f"{stem}.pdf"


def _open_rgb_image(image_bytes: bytes) -> Image.Image:
    if not image_bytes:
        raise ValueError("Image file is empty.")
    try:
        image = Image.open(io.BytesIO(image_bytes))
        image.load()
    except UnidentifiedImageError as exc:
        raise ValueError("File is not a valid PNG or JPEG image.") from exc
    except OSError as exc:
        raise ValueError("Could not read the image file.") from exc

    fmt = (image.format or "").upper()
    if fmt not in {"PNG", "JPEG", "JPG"}:
        raise ValueError("Only PNG and JPEG images can be converted to PDF.")

    if max(image.size) > MAX_IMAGE_EDGE_PX:
        image.thumbnail((MAX_IMAGE_EDGE_PX, MAX_IMAGE_EDGE_PX), Image.Resampling.LANCZOS)

    if image.mode in ("RGBA", "LA") or (image.mode == "P" and "transparency" in image.info):
        rgba = image.convert("RGBA")
        background = Image.new("RGB", rgba.size, (255, 255, 255))
        background.paste(rgba, mask=rgba.split()[-1])
        return background
    return image.convert("RGB")


def image_bytes_to_pdf(image_bytes: bytes, original_filename: str) -> Tuple[bytes, str]:
    """
    Fit the image on a single A4 page and return (pdf_bytes, pdf_filename).
    """
    image = _open_rgb_image(image_bytes)
    image_buf = io.BytesIO()
    image.save(image_buf, format="JPEG", quality=90, optimize=True)
    image_buf.seek(0)

    page_w, page_h = A4
    max_w = page_w - (PAGE_MARGIN_PT * 2)
    max_h = page_h - (PAGE_MARGIN_PT * 2)
    img_w, img_h = image.size
    scale = min(max_w / img_w, max_h / img_h)
    draw_w = img_w * scale
    draw_h = img_h * scale
    x = (page_w - draw_w) / 2
    y = (page_h - draw_h) / 2

    pdf_buf = io.BytesIO()
    pdf = canvas.Canvas(pdf_buf, pagesize=A4)
    pdf.drawImage(
        ImageReader(image_buf),
        x,
        y,
        width=draw_w,
        height=draw_h,
        preserveAspectRatio=True,
        mask="auto",
    )
    pdf.save()
    return pdf_buf.getvalue(), pdf_filename_from_image(original_filename)
