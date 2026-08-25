"""Unit tests for PNG/JPEG → A4 PDF conversion (no network)."""

import io

from PIL import Image

from tools.image_to_pdf import image_bytes_to_pdf, is_image_filename, pdf_filename_from_image


def _png_bytes(mode: str = "RGB", size=(80, 50), color=(20, 80, 160)) -> bytes:
    image = Image.new(mode, size, color if mode != "RGBA" else (*color, 128))
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return buf.getvalue()


def _jpeg_bytes(size=(60, 90), color=(200, 40, 40)) -> bytes:
    image = Image.new("RGB", size, color)
    buf = io.BytesIO()
    image.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def test_is_image_filename():
    assert is_image_filename("scan.PNG")
    assert is_image_filename("photo.jpeg")
    assert is_image_filename("photo.jpg")
    assert not is_image_filename("letter.pdf")
    assert not is_image_filename("letter.docx")


def test_pdf_filename_replaces_image_extension():
    assert pdf_filename_from_image("Hawthorn scan.PNG") == "Hawthorn scan.pdf"
    assert pdf_filename_from_image("photo.jpg") == "photo.pdf"


def test_png_converts_to_pdf_bytes():
    pdf_bytes, name = image_bytes_to_pdf(_png_bytes(), "member-testimonial.png")
    assert name == "member-testimonial.pdf"
    assert pdf_bytes.startswith(b"%PDF")


def test_jpeg_converts_to_pdf_bytes():
    pdf_bytes, name = image_bytes_to_pdf(_jpeg_bytes(), "scan.jpg")
    assert name.endswith(".pdf")
    assert pdf_bytes.startswith(b"%PDF")


def test_transparent_png_converts():
    pdf_bytes, _name = image_bytes_to_pdf(_png_bytes(mode="RGBA"), "badge.png")
    assert pdf_bytes.startswith(b"%PDF")


def test_invalid_bytes_raise():
    try:
        image_bytes_to_pdf(b"not-an-image", "fake.png")
    except ValueError as exc:
        assert "valid PNG or JPEG" in str(exc)
    else:
        raise AssertionError("expected ValueError")
