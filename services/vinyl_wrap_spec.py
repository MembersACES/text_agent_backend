"""
Deterministic PUDU CC1 vinyl wrap mockup board (SVG).

Visual modes (wrap_style):
- commercial (default): clean fills + soft accent wash.
- sports: sunburst rays + stronger accent.

Layout presets (layout_preset) — how logos sit on the flat templates:
- generic: mirrored logos on front wings; single logo centred below rear cutout.
- ics: single centred logo front and back (corporate / “ICS-style”).
- foodworks: centred hero on front; mirrored pair on rear shell (retail wrap).
- sports_showcase: sunburst + wing logos + optional partner badge (first extra colour).

generate_vinyl_wrap_spec_payload returns multiple variants so the user can compare.

Full PUDU CC1 body-skin kit on the board:
  • Front shell 1164×469, back wrap 1045×489
  • Charging + key covers 71×48 each (separate art — also cut through on main front as holes)
  • Rear centre stack: 293×143, 275×110, 303×229.5 mm (rectangular placeholders)
  • Optional vertical fold line on back wrap (per factory note)
"""
from __future__ import annotations

import base64
import json
import math
import re
from typing import Dict, List, Optional
from xml.sax.saxutils import escape

MAX_LOGO_BYTES = 2 * 1024 * 1024
_HEX_RE = re.compile(r"^#?([0-9A-Fa-f]{6})$")
_FILENAME_SAFE_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]+')


def normalize_hex(color: str) -> str:
    s = (color or "").strip()
    m = _HEX_RE.match(s)
    if not m:
        raise ValueError("invalid hex colour")
    return f"#{m.group(1).upper()}"


def validate_hex(color: str, field_name: str) -> str:
    try:
        return normalize_hex(color)
    except ValueError as e:
        raise ValueError(f"{field_name}: {e}") from e


def escape_xml(text: str) -> str:
    return escape(str(text), {"'": "&apos;", '"': "&quot;"})


def _sanitize_filename_part(name: str, max_len: int = 72) -> str:
    s = _FILENAME_SAFE_RE.sub("_", (name or "").strip()) or "Client"
    return s[:max_len].rstrip("._ ")


def _parse_extra_colours(raw: str) -> List[Dict[str, str]]:
    if not raw or not str(raw).strip():
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError("extra_colours_json must be valid JSON") from e
    if not isinstance(data, list):
        raise ValueError("extra_colours_json must be a JSON array")
    out: List[Dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        label = str(item.get("label", "")).strip()
        hx = str(item.get("hex", "")).strip()
        if not label or not hx:
            continue
        out.append({"label": label, "hex": validate_hex(hx, "extra_colours.hex")})
    return out


def _hex_to_rgb(h: str) -> tuple:
    h = h.lstrip("#")
    return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))


def _darken(h: str, factor: float = 0.7) -> str:
    r, g, b = _hex_to_rgb(h)
    return "#{:02X}{:02X}{:02X}".format(int(r * factor), int(g * factor), int(b * factor))


# ─────────────────────────────────────────────────────────────────────────────
# FRONT SHELL  —  1164 × 469 mm
#
# Shape traced from official PUDU CC1 spec sheet:
#  • Classic bowtie / butterfly silhouette
#  • Bottom edge is very wide (full ~1164px)
#  • Sides curve strongly INWARD creating a narrow waist around mid-height
#  • Top has two narrow shoulders that connect to the U-arch neck
#  • Wide U-arch notch cut into the top centre (sensor-head gap)
#  • Each wing has 2 small pill cutouts (charging cover + key cover) near bottom
# ─────────────────────────────────────────────────────────────────────────────
FRONT_W, FRONT_H = 1164, 469

# Outer butterfly silhouette — single closed clockwise path.
# Canvas is 1164 × 469. Centre x = 582.
# Key shape: classic bowtie. Bottom is widest, sides curve in to a narrow waist
# around y=220, then expand again to the shoulder blades at top before meeting the
# narrow U arch neck. Perfectly symmetric around x=582.
FRONT_SHELL_PATH = (
    # ── start bottom-left ──────────────────────────────────────────────────────────
    "M 40 455 "
    # left outer edge rounded corner
    "C 8 455, 0 442, 2 420 "
    # left side curves inward (creating the waist around y=220)
    "C 4 360, 20 280, 60 220 "
    "C 100 160, 170 110, 248 76 "
    # left shoulder — sweeps up and inward sharply toward the neck
    "C 326 42, 416 18, 488 14 "
    "C 524 12, 546 22, 552 42 "
    "L 548 74 "
    # ── left wall of U arch notch ────────────────────────────────────────────
    "L 548 140 "
    "C 548 162, 560 176, 576 178 "
    # ── U arch floor gap (~80px wide, centred at x=582) ───────────────────────
    "L 612 178 "
    # ── right wall of U arch notch ───────────────────────────────────────────
    "C 628 176, 640 162, 640 140 "
    "L 640 74 "
    "L 644 42 "
    "C 638 22, 660 12, 696 14 "
    # right shoulder heading outward
    "C 768 18, 858 42, 936 76 "
    "C 1014 110, 1084 160, 1124 220 "
    # right outer edge curves outward and down
    "C 1164 280, 1180 360, 1182 420 "
    "C 1184 442, 1176 455, 1144 455 "
    # ── lower front sweep (very broad / flatter, so colour reads fully filled) ──
    "C 1080 462, 920 464, 740 460 "
    "C 560 456, 410 454, 300 456 "
    "C 220 458, 150 458, 100 455 "
    "Z"
)

# Front shell should render as a fully filled artwork piece.
# The 71×48 charging/key pieces are provided separately in the additional-stickers row,
# so we do not punch white holes in the main front panel.
def _front_wing_cutouts():
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# BACK SHELL  —  1045 × 489 mm
#
# Shape from spec sheet + Epworth / FoodWorks / AKL references:
#  • ONE single piece — trapezoidal outer, WIDER at bottom, angled sides
#  • Top is narrower, bottom is full width
#  • Wide U arch notch cut from the top centre (creates two shoulder lobes)
#  • ONE square-ish charging port cutout, CENTRED horizontally, in upper half
#  • Logo goes below the charging cutout, horizontally centred
# ─────────────────────────────────────────────────────────────────────────────
BACK_W, BACK_H = 1045, 489

# Additional PUDU CC1 1:1 (mm) pieces from official body-skin spec sheet
COVER_W, COVER_H = 71, 48
# Rear centre “stack” — rectangular placeholders; verify complex cut paths in vendor CAD.
REAR_CENTER_PIECE_A = (293, 143)   # w × h (mm)
REAR_CENTER_PIECE_B = (275, 110)
REAR_CENTER_PIECE_C = (303, 229.5)

# Outer shape: trapezoid wider at bottom, angled sides.
# Top edge: x=180..865 (685px wide), bottom edge: x=0..1045 (full width)
# Using even-odd fill — U arch subpath punches the notch hole at top centre.
BACK_SHELL_OUTER = (
    # top-left shoulder
    "M 180 0 "
    "L 865 0 "
    # right side angles outward toward full-width bottom
    "C 920 0, 970 18, 1000 50 "
    "L 1040 440 "
    "C 1042 458, 1028 472, 1010 472 "
    "L 35 472 "
    "C 17 472, 3 458, 5 440 "
    "L 45 50 "
    "C 75 18, 125 0, 180 0 "
    "Z"
)

# U arch hole — wide notch cut from the top centre.
# Walls are roughly vertical, floor rounded at y≈200.
# Spans x=340..705 (365px wide), centred at x=522.
BACK_SHELL_UARCH = (
    "M 340 0 "
    "L 705 0 "
    "L 705 185 "
    "C 705 206, 688 218, 668 218 "
    "L 376 218 "
    "C 356 218, 340 206, 340 185 "
    "Z"
)

BACK_SHELL_PATH = BACK_SHELL_OUTER + " " + BACK_SHELL_UARCH

# Charging port cutout — square-ish rounded rect, CENTRED at x=522.
# Sits in the upper-centre of the body below the U arch floor (y≈218).
# Proportioned to leave a good strip below for the logo (cutout y=234..370).
BACK_CHARGING_CUTOUT = (
    "M 322 248 "
    "C 322 236, 332 228, 346 228 "
    "L 698 228 "
    "C 712 228, 722 236, 722 248 "
    "L 722 372 "
    "C 722 384, 712 392, 698 392 "
    "L 346 392 "
    "C 332 392, 322 384, 322 372 "
    "Z"
)


LAYOUT_PRESETS = ("generic", "ics", "foodworks", "sports_showcase")

VALID_LAYOUT_PRESETS = frozenset(LAYOUT_PRESETS)

# Shown on each generated spec board (subtitle under client name).
VARIANT_BOARD_LABELS = {
    "generic": "Layout A · Dual-wing — mirrors your Commercial / Sports texture choice",
    "ics": "Layout B · Centre brand — single hero front & back (corporate)",
    "foodworks": "Layout C · Retail — centred front; mirrored pair on rear shell",
    "sports_showcase": "Layout D · Sports / event — sunburst + wing logos + partner badge",
}


def _partner_hex_from_extras(extra_colours: List[Dict[str, str]], secondary_hex: str) -> str:
    if extra_colours and isinstance(extra_colours[0], dict):
        hx = str(extra_colours[0].get("hex", "")).strip()
        if hx:
            try:
                return validate_hex(hx, "partner")
            except ValueError:
                pass
    return secondary_hex


def _wrap_for_preset(layout_preset: str, user_wrap_style: str) -> str:
    """Sports showcase always uses sports texture; ICS/foodworks use commercial on flats."""
    lp = (layout_preset or "generic").strip().lower()
    if lp == "sports_showcase":
        return "sports"
    if lp in ("ics", "foodworks"):
        return "commercial"
    return (user_wrap_style or "commercial").strip().lower()


def sunburst_rays(cx, cy, inner_r, outer_r, ray_count, ray_width_deg, colour, opacity=0.3):
    rays = []
    step = 360.0 / ray_count
    for i in range(ray_count):
        angle = i * step - 90
        half = ray_width_deg / 2
        a1 = math.radians(angle - half)
        a2 = math.radians(angle + half)
        x1 = cx + inner_r * math.cos(a1); y1 = cy + inner_r * math.sin(a1)
        x2 = cx + inner_r * math.cos(a2); y2 = cy + inner_r * math.sin(a2)
        x3 = cx + outer_r * math.cos(a2); y3 = cy + outer_r * math.sin(a2)
        x4 = cx + outer_r * math.cos(a1); y4 = cy + outer_r * math.sin(a1)
        pts = f"{x1:.1f},{y1:.1f} {x2:.1f},{y2:.1f} {x3:.1f},{y3:.1f} {x4:.1f},{y4:.1f}"
        rays.append(f'<polygon points="{pts}" fill="{colour}" opacity="{opacity}"/>')
    return "\n      ".join(rays)


# ─────────────────────────────────────────────────────────────────────────────
# FRONT DECAL
# ─────────────────────────────────────────────────────────────────────────────
def front_decal(x, y, scale, primary, secondary, text_hex, logo_data_url, wrap_style,
                layout_preset="generic", partner_hex=None):
    clip_id  = "frontClip"
    grad_id  = "frontGrad"
    dark     = _darken(primary, 0.72)
    sports   = (wrap_style or "commercial").strip().lower() == "sports"
    lp       = (layout_preset or "generic").strip().lower()
    partner  = partner_hex or secondary
    # FRONT_SHELL_PATH intentionally overshoots x bounds (up to ~1184), so
    # paint layers must overfill beyond 0..1164 to avoid edge "unfilled" slivers.
    fill_x = -40
    fill_w = FRONT_W + 80

    def _logo(cx, cy, w=300, h=160):
        if logo_data_url:
            return (f'<image href="{escape_xml(logo_data_url)}" '
                    f'x="{cx-w//2}" y="{cy-h//2}" width="{w}" height="{h}" '
                    f'preserveAspectRatio="xMidYMid meet"/>')
        return (f'<text x="{cx}" y="{cy+14}" text-anchor="middle" '
                f'fill="{text_hex}" font-family="Syne,Arial,sans-serif" '
                f'font-size="72" font-weight="800" letter-spacing="2">LOGO</text>')

    def _partner_badges_front():
        """Small circular “partner” marks above each wing (sports / event look)."""
        r = 26
        return (
            f'<circle cx="170" cy="208" r="{r}" fill="{partner}" stroke="#ffffff" stroke-width="3" opacity="0.95"/>'
            f'<circle cx="994" cy="208" r="{r}" fill="{partner}" stroke="#ffffff" stroke-width="3" opacity="0.95"/>'
        )

    if lp in ("ics", "foodworks"):
        # Single hero centred on the chest (between wings).
        logos = _logo(582, 252, w=460, h=210)
        accent = (f'<rect x="{fill_x}" y="0" width="{fill_w}" height="{FRONT_H}" '
                  f'fill="{secondary}" opacity="0.05"/>')
        partner_marks = ""
    elif lp == "sports_showcase":
        logos = _logo(170, 290, w=300, h=160) + "\n      " + _logo(994, 290, w=300, h=160)
        accent = (sunburst_rays(170, 290, 20, 900, 24, 6, secondary, 0.14) +
                  "\n      " +
                  sunburst_rays(994, 290, 20, 900, 24, 6, secondary, 0.14))
        partner_marks = _partner_badges_front()
    else:
        # generic — mirrored wing logos
        logos = _logo(170, 290) + "\n      " + _logo(994, 290)
        partner_marks = ""
        if sports:
            accent = (sunburst_rays(170, 290, 20, 900, 24, 6, secondary, 0.14) +
                      "\n      " +
                      sunburst_rays(994, 290, 20, 900, 24, 6, secondary, 0.14))
        else:
            accent = (f'<rect x="{fill_x}" y="0" width="{fill_w}" height="{FRONT_H}" '
                      f'fill="{secondary}" opacity="0.06"/>')

    cuts = _front_wing_cutouts()

    return f"""
  <g transform="translate({x},{y}) scale({scale})">
    <defs>
      <clipPath id="{clip_id}"><path d="{FRONT_SHELL_PATH}"/></clipPath>
      <radialGradient id="{grad_id}" cx="50%" cy="55%" r="72%">
        <stop offset="0%"   stop-color="{primary}"/>
        <stop offset="70%"  stop-color="{primary}"/>
        <stop offset="100%" stop-color="{dark}"/>
      </radialGradient>
    </defs>
    <g clip-path="url(#{clip_id})">
      <rect x="{fill_x}" y="0" width="{fill_w}" height="{FRONT_H}" fill="url(#{grad_id})"/>
      {accent}
      {partner_marks}
      {logos}
      {cuts}
    </g>
    <path d="{FRONT_SHELL_PATH}" fill="none" stroke="#1a1a1a" stroke-width="2" stroke-linejoin="round"/>
  </g>"""


# ─────────────────────────────────────────────────────────────────────────────
# BACK DECAL
# ─────────────────────────────────────────────────────────────────────────────
def back_decal(x, y, scale, primary, secondary, text_hex, logo_data_url, wrap_style,
               layout_preset="generic", partner_hex=None):
    clip_id = "backClip"
    grad_id = "backGrad"
    dark    = _darken(primary, 0.72)
    sports  = (wrap_style or "commercial").strip().lower() == "sports"
    lp      = (layout_preset or "generic").strip().lower()
    partner = partner_hex or secondary

    def _logo(cx, cy, w=480, h=120):
        if logo_data_url:
            return (f'<image href="{escape_xml(logo_data_url)}" '
                    f'x="{cx-w//2}" y="{cy-h//2}" width="{w}" height="{h}" '
                    f'preserveAspectRatio="xMidYMid meet"/>')
        return (f'<text x="{cx}" y="{cy+14}" text-anchor="middle" '
                f'fill="{text_hex}" font-family="Syne,Arial,sans-serif" '
                f'font-size="72" font-weight="800" letter-spacing="2">LOGO</text>')

    # Logo centred below the charging cutout (cutout bottom y=392, shell bottom y=472)
    if lp == "foodworks":
        # Mirrored retail stack on left / right lobes of the lower shell.
        logo = _logo(310, 438, w=232, h=84) + "\n      " + _logo(734, 438, w=232, h=84)
        accent = (f'<rect x="0" y="0" width="{BACK_W}" height="{BACK_H}" '
                  f'fill="{secondary}" opacity="0.05"/>')
    elif lp == "sports_showcase":
        logo = _logo(310, 438, w=244, h=86) + "\n      " + _logo(734, 438, w=244, h=86)
        accent = sunburst_rays(522, 360, 20, 900, 24, 6, secondary, 0.20)
        logo = (
            f'<circle cx="310" cy="350" r="24" fill="{partner}" stroke="#ffffff" stroke-width="2.5" opacity="0.95"/>'
            f'\n      <circle cx="734" cy="350" r="24" fill="{partner}" stroke="#ffffff" stroke-width="2.5" opacity="0.95"/>'
            f"\n      {logo}"
        )
    else:
        # generic + ics — single block under the charging port
        logo = _logo(522, 440, w=480, h=88)
        if sports:
            accent = sunburst_rays(522, 360, 20, 900, 24, 6, secondary, 0.20)
        else:
            accent = (f'<rect x="0" y="0" width="{BACK_W}" height="{BACK_H}" '
                      f'fill="{secondary}" opacity="0.06"/>')

    cutout = f'<path d="{BACK_CHARGING_CUTOUT}" fill="white"/>'

    return f"""
  <g transform="translate({x},{y}) scale({scale})">
    <defs>
      <clipPath id="{clip_id}"><path d="{BACK_SHELL_PATH}" fill-rule="evenodd"/></clipPath>
      <radialGradient id="{grad_id}" cx="50%" cy="55%" r="72%">
        <stop offset="0%"   stop-color="{primary}"/>
        <stop offset="70%"  stop-color="{primary}"/>
        <stop offset="100%" stop-color="{dark}"/>
      </radialGradient>
    </defs>
    <g clip-path="url(#{clip_id})">
      <rect x="0" y="0" width="{BACK_W}" height="{BACK_H}" fill="url(#{grad_id})"/>
      {accent}
      {logo}
      {cutout}
    </g>
    <path d="{BACK_SHELL_PATH}" fill="none" stroke="#1a1a1a" stroke-width="2" stroke-linejoin="round" fill-rule="evenodd"/>
    <!-- PUDU spec: optional vertical fold — kept subtle so it does not compete with artwork -->
    <line x1="522" y1="28" x2="522" y2="438" stroke="#b8c0d0" stroke-width="0.65" stroke-dasharray="4 6" opacity="0.4"/>
    <text x="522" y="16" text-anchor="middle" fill="#9aa6b8" font-family="DM Sans,Arial,sans-serif" font-size="7" font-style="italic">optional fold</text>
    <!-- Charging port — label inside shell bounds (was left of x=0 and clipped after scale) -->
    <text x="330" y="222" text-anchor="start" fill="#5c6578"
          font-family="DM Sans,Arial,sans-serif" font-size="10" font-style="italic">Charging port</text>
  </g>"""


# ─────────────────────────────────────────────────────────────────────────────
# ROBOT SILHOUETTES
#
# Key real-world proportions for PUDU CC1:
#  FRONT VIEW:
#   • Body is an INVERTED TRAPEZOID — wider at top, narrows toward bottom
#   • Black sensor head sits on top (separate piece, not wrapped)
#   • Lime-green fixed side panel (factory plastic, not part of the vinyl) on the RIGHT
#   • Grey base / wheel unit at the bottom
#
#  REAR VIEW:
#   • Same inverted trapezoid body
#   • Dark grey lid on top
#   • Same fixed lime side panel on the LEFT in rear view (viewer's left = robot's right)
#   • Same grey base
# ─────────────────────────────────────────────────────────────────────────────

def robot_front(x, y, scale, primary, secondary, text_hex, logo_data_url,
                suffix="f", wrap_style="commercial", layout_preset="generic",
                partner_hex=None):
    bg   = f"bodyGrad_{suffix}"
    bc   = f"bodyClip_{suffix}"
    gg   = f"greenGrad_{suffix}"
    base = f"baseGrad_{suffix}"
    shad = f"shadowGrad_{suffix}"
    orb  = f"icsOrb_{suffix}"
    dark = _darken(primary, 0.68)
    sports = (wrap_style or "commercial").strip().lower() == "sports"
    lp = (layout_preset or "generic").strip().lower()
    partner = partner_hex or secondary

    # Body: inverted trapezoid — top width 340, bottom width 260, height 350
    # Top-left: (60,190), Top-right: (400,190), Bottom-right: (370,540), Bottom-left:(90,540)
    body_path = "M 60 190 L 400 190 L 370 540 L 90 540 Z"

    ics_orb_def = ""
    ics_orb_shape = ""

    if lp == "ics":
        if logo_data_url:
            logo = (f'<image href="{escape_xml(logo_data_url)}" '
                    f'x="78" y="278" width="284" height="188" preserveAspectRatio="xMidYMid meet"/>')
        else:
            logo = (f'<text x="230" y="395" text-anchor="middle" fill="{text_hex}" '
                    f'font-family="Syne,Arial,sans-serif" font-size="36" font-weight="800">LOGO</text>')
        ics_orb_def = f"""
      <radialGradient id="{orb}" cx="40%" cy="35%" r="65%">
        <stop offset="0%"   stop-color="{_darken(primary, 0.45)}"/>
        <stop offset="55%"  stop-color="{_darken(primary, 0.72)}"/>
        <stop offset="100%" stop-color="{_darken(primary, 0.88)}"/>
      </radialGradient>"""
        ics_orb_shape = (
            f'<ellipse cx="220" cy="378" rx="102" ry="86" fill="url(#{orb})" opacity="0.55"/>'
        )
    elif lp == "foodworks":
        if logo_data_url:
            logo = (f'<image href="{escape_xml(logo_data_url)}" '
                    f'x="78" y="278" width="284" height="188" preserveAspectRatio="xMidYMid meet"/>')
        else:
            logo = (f'<text x="230" y="395" text-anchor="middle" fill="{text_hex}" '
                    f'font-family="Syne,Arial,sans-serif" font-size="36" font-weight="800">LOGO</text>')
    elif lp == "sports_showcase":
        if logo_data_url:
            esc = escape_xml(logo_data_url)
            logo = (
                f'<circle cx="145" cy="318" r="18" fill="{partner}" stroke="#fff" stroke-width="2"/>'
                f'<circle cx="315" cy="318" r="18" fill="{partner}" stroke="#fff" stroke-width="2"/>'
                f'<image href="{esc}" x="81" y="334" width="128" height="88" preserveAspectRatio="xMidYMid meet"/>'
                f'<image href="{esc}" x="251" y="334" width="128" height="88" preserveAspectRatio="xMidYMid meet"/>'
            )
        else:
            logo = (
                f'<text x="145" y="395" text-anchor="middle" fill="{text_hex}" '
                f'font-family="Syne,Arial,sans-serif" font-size="22" font-weight="800">LOGO</text>'
                f'<text x="315" y="395" text-anchor="middle" fill="{text_hex}" '
                f'font-family="Syne,Arial,sans-serif" font-size="22" font-weight="800">LOGO</text>'
            )
    else:
        if logo_data_url:
            logo = (f'<image href="{escape_xml(logo_data_url)}" '
                    f'x="92" y="300" width="256" height="160" preserveAspectRatio="xMidYMid meet"/>')
        else:
            logo = (f'<text x="230" y="395" text-anchor="middle" fill="{text_hex}" '
                    f'font-family="Syne,Arial,sans-serif" font-size="36" font-weight="800">LOGO</text>')

    body_texture = sunburst_rays(230, 365, 18, 480, 20, 6, secondary, 0.20) if sports else ""
    if lp in ("ics", "foodworks", "sports_showcase"):
        plate = ""
    else:
        plate = ("" if sports else
                 '<rect x="92" y="282" width="256" height="178" rx="14" fill="#000" opacity="0.14"/>')

    return f"""
  <g transform="translate({x},{y}) scale({scale})">
    <defs>
      <clipPath id="{bc}"><path d="{body_path}"/></clipPath>
      {ics_orb_def}
      <linearGradient id="{bg}" x1="0" y1="0" x2="1" y2="0">
        <stop offset="0%"   stop-color="{dark}"/>
        <stop offset="20%"  stop-color="{primary}"/>
        <stop offset="80%"  stop-color="{primary}"/>
        <stop offset="100%" stop-color="{dark}"/>
      </linearGradient>
      <linearGradient id="{gg}" x1="0" y1="0" x2="1" y2="0">
        <stop offset="0%"   stop-color="#8fd43a"/>
        <stop offset="100%" stop-color="#6bb52f"/>
      </linearGradient>
      <linearGradient id="{base}" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"   stop-color="#d4d4d8"/>
        <stop offset="100%" stop-color="#9a9aa2"/>
      </linearGradient>
      <radialGradient id="{shad}" cx="50%" cy="50%" r="50%">
        <stop offset="0%"   stop-color="#000" stop-opacity="0.28"/>
        <stop offset="100%" stop-color="#000" stop-opacity="0"/>
      </radialGradient>
    </defs>

    <!-- ground shadow -->
    <ellipse cx="230" cy="666" rx="196" ry="15" fill="url(#{shad})"/>

    <!-- wheel/base unit -->
    <rect x="42" y="604" width="396" height="50" rx="12" fill="#28303a"/>
    <rect x="66" y="618" width="152" height="22" rx="5"  fill="#363d47"/>
    <rect x="262" y="618" width="152" height="22" rx="5" fill="#363d47"/>
    <ellipse cx="100" cy="654" rx="46" ry="22" fill="#1c2028"/>
    <ellipse cx="360" cy="654" rx="46" ry="22" fill="#1c2028"/>
    <ellipse cx="100" cy="654" rx="28" ry="13" fill="#282e38"/>
    <ellipse cx="360" cy="654" rx="28" ry="13" fill="#282e38"/>

    <!-- base skirt (grey band between body and wheels) -->
    <path d="M 78 540 L 78 608 L 382 608 L 382 540 C 380 530 370 526 358 526 L 102 526 C 90 526 80 530 78 540 Z"
          fill="url(#{base})" stroke="#a0a0a8" stroke-width="0.8"/>
    <!-- brush strip -->
    <rect x="42" y="600" width="396" height="8" rx="2" fill="#1c2028" opacity="0.6"/>

    <!-- wrapped body -->
    <g clip-path="url(#{bc})">
      <rect x="60" y="190" width="340" height="350" fill="url(#{bg})"/>
      {body_texture}
      {plate}
      {ics_orb_shape}
      {logo}
      <!-- top highlight -->
      <rect x="60" y="190" width="340" height="16" fill="white" opacity="0.10"/>
    </g>
    <path d="{body_path}" fill="none" stroke="#111" stroke-width="1.4"/>

    <!-- lime-green right side panel (viewer's right), flush to body edge -->
    <path d="M 400 190 L 454 206 Q 466 212 466 224 L 464 536 Q 464 548 452 550 L 400 542 Q 390 538 388 530 L 388 196 Q 390 188 400 190 Z"
          fill="url(#{gg})" stroke="#4a8e1f" stroke-width="1"/>
    <rect x="408" y="360" width="44" height="42" rx="5" fill="#c0d890" stroke="#7aae30" stroke-width="0.8"/>
    <rect x="410" y="418" width="40" height="13" rx="3" fill="#a0c860"/>

    <!-- black sensor head -->
    <path d="M 70 18 Q 66 4, 88 2 L 372 2 Q 394 4, 390 18 L 386 174 Q 386 190, 364 192 L 96 192 Q 74 190, 70 174 Z"
          fill="#0c0c0c" stroke="#000" stroke-width="1.2"/>
    <rect x="96" y="30" width="264" height="148" rx="14" fill="#060606"/>
    <!-- left eye -->
    <rect x="116" y="48" width="90" height="108" rx="18" fill="#080808"/>
    <ellipse cx="161" cy="102" rx="30" ry="34" fill="#fff" opacity="0.92"/>
    <ellipse cx="163" cy="99"  rx="11" ry="12" fill="#000"/>
    <ellipse cx="168" cy="94"  rx="4"  ry="4"  fill="#fff" opacity="0.7"/>
    <!-- right eye -->
    <rect x="250" y="48" width="90" height="108" rx="18" fill="#080808"/>
    <ellipse cx="295" cy="102" rx="30" ry="34" fill="#fff" opacity="0.92"/>
    <ellipse cx="297" cy="99"  rx="11" ry="12" fill="#000"/>
    <ellipse cx="302" cy="94"  rx="4"  ry="4"  fill="#fff" opacity="0.7"/>
    <!-- head highlight -->
    <path d="M 70 18 Q 66 4, 88 2 L 372 2 Q 394 4, 390 18 L 384 30 L 76 30 Z"
          fill="#fff" opacity="0.06"/>

    <!-- charging port strip at base of body -->
    <rect x="110" y="556" width="196" height="22" rx="4" fill="#e0e0e4" stroke="#a0a0a8" stroke-width="0.6"/>
    <rect x="114" y="560" width="44" height="14" rx="3" fill="#c0c0c4"/>
    <rect x="164" y="560" width="44" height="14" rx="3" fill="#c0c0c4"/>
    <rect x="214" y="560" width="44" height="14" rx="3" fill="#c0c0c4"/>
    <rect x="264" y="560" width="36" height="14" rx="3" fill="#c0c0c4"/>
  </g>"""


def robot_rear(x, y, scale, primary, secondary, text_hex, logo_data_url,
               suffix="r", wrap_style="commercial", layout_preset="generic",
               partner_hex=None):
    bg   = f"bodyGrad_{suffix}"
    bc   = f"bodyClip_{suffix}"
    gg   = f"greenGrad_{suffix}"
    base = f"baseGrad_{suffix}"
    shad = f"shadowGrad_{suffix}"
    tg   = f"topGrad_{suffix}"
    dark = _darken(primary, 0.68)
    sports = (wrap_style or "commercial").strip().lower() == "sports"
    lp = (layout_preset or "generic").strip().lower()
    partner = partner_hex or secondary

    # Rear body: same inverted trapezoid
    body_path = "M 48 162 L 362 162 L 334 510 L 76 510 Z"

    if lp in ("foodworks", "sports_showcase") and logo_data_url:
        esc = escape_xml(logo_data_url)
        if lp == "foodworks":
            logo = (
                f'<image href="{esc}" x="54" y="278" width="118" height="78" preserveAspectRatio="xMidYMid meet"/>'
                f'<image href="{esc}" x="232" y="278" width="118" height="78" preserveAspectRatio="xMidYMid meet"/>'
            )
        else:
            logo = (
                f'<circle cx="112" cy="248" r="14" fill="{partner}" stroke="#fff" stroke-width="1.8"/>'
                f'<circle cx="292" cy="248" r="14" fill="{partner}" stroke="#fff" stroke-width="1.8"/>'
                f'<image href="{esc}" x="54" y="278" width="118" height="78" preserveAspectRatio="xMidYMid meet"/>'
                f'<image href="{esc}" x="232" y="278" width="118" height="78" preserveAspectRatio="xMidYMid meet"/>'
            )
    elif logo_data_url:
        logo = (f'<image href="{escape_xml(logo_data_url)}" '
                f'x="76" y="268" width="246" height="158" preserveAspectRatio="xMidYMid meet"/>')
    else:
        if lp in ("foodworks", "sports_showcase"):
            logo = (
                f'<text x="112" y="330" text-anchor="middle" fill="{text_hex}" '
                f'font-family="Syne,Arial,sans-serif" font-size="18" font-weight="800">LOGO</text>'
                f'<text x="292" y="330" text-anchor="middle" fill="{text_hex}" '
                f'font-family="Syne,Arial,sans-serif" font-size="18" font-weight="800">LOGO</text>'
            )
        else:
            logo = (f'<text x="205" y="358" text-anchor="middle" fill="{text_hex}" '
                    f'font-family="Syne,Arial,sans-serif" font-size="34" font-weight="800">LOGO</text>')

    body_texture = sunburst_rays(205, 336, 18, 450, 20, 6, secondary, 0.20) if sports else ""
    if lp in ("ics", "foodworks", "sports_showcase"):
        plate = ""
    else:
        plate = ("" if sports else
                 '<rect x="76" y="250" width="246" height="178" rx="14" fill="#000" opacity="0.14"/>')

    return f"""
  <g transform="translate({x},{y}) scale({scale})">
    <defs>
      <clipPath id="{bc}"><path d="{body_path}"/></clipPath>
      <linearGradient id="{bg}" x1="0" y1="0" x2="1" y2="0">
        <stop offset="0%"   stop-color="{dark}"/>
        <stop offset="20%"  stop-color="{primary}"/>
        <stop offset="80%"  stop-color="{primary}"/>
        <stop offset="100%" stop-color="{dark}"/>
      </linearGradient>
      <linearGradient id="{gg}" x1="0" y1="0" x2="1" y2="0">
        <stop offset="0%"   stop-color="#8fd43a"/>
        <stop offset="100%" stop-color="#6bb52f"/>
      </linearGradient>
      <linearGradient id="{base}" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"   stop-color="#d4d4d8"/>
        <stop offset="100%" stop-color="#9a9aa2"/>
      </linearGradient>
      <linearGradient id="{tg}" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"   stop-color="#3c4148"/>
        <stop offset="100%" stop-color="#242930"/>
      </linearGradient>
      <radialGradient id="{shad}" cx="50%" cy="50%" r="50%">
        <stop offset="0%"   stop-color="#000" stop-opacity="0.28"/>
        <stop offset="100%" stop-color="#000" stop-opacity="0"/>
      </radialGradient>
    </defs>

    <!-- ground shadow -->
    <ellipse cx="205" cy="628" rx="178" ry="14" fill="url(#{shad})"/>

    <!-- wheel/base -->
    <rect x="28" y="568" width="364" height="48" rx="10" fill="#28303a"/>
    <rect x="52" y="582" width="138" height="20" rx="5"  fill="#363d47"/>
    <rect x="230" y="582" width="138" height="20" rx="5" fill="#363d47"/>
    <ellipse cx="84"  cy="616" rx="40" ry="20" fill="#1c2028"/>
    <ellipse cx="328" cy="616" rx="40" ry="20" fill="#1c2028"/>
    <ellipse cx="84"  cy="616" rx="24" ry="12" fill="#282e38"/>
    <ellipse cx="328" cy="616" rx="24" ry="12" fill="#282e38"/>

    <!-- base skirt -->
    <path d="M 64 510 L 64 572 L 348 572 L 348 510 C 346 500 338 496 326 496 L 86 496 C 74 496 66 500 64 510 Z"
          fill="url(#{base})" stroke="#a0a0a8" stroke-width="0.8"/>
    <rect x="28" y="564" width="364" height="8" rx="2" fill="#1c2028" opacity="0.6"/>

    <!-- dark top lid -->
    <path d="M 48 56 Q 46 40, 68 36 L 344 36 Q 366 40, 364 56 L 364 166 L 48 166 Z"
          fill="url(#{tg})"/>
    <!-- power indicator -->
    <circle cx="334" cy="58" r="7" fill="#d01818"/>
    <circle cx="334" cy="58" r="3.5" fill="#f04040"/>
    <!-- lid highlight -->
    <path d="M 48 56 Q 46 40, 68 36 L 344 36 Q 366 40, 364 56 L 358 68 L 54 68 Z"
          fill="#fff" opacity="0.07"/>

    <!-- wrapped body -->
    <g clip-path="url(#{bc})">
      <rect x="48" y="162" width="314" height="348" fill="url(#{bg})"/>
      {body_texture}
      {plate}
      {logo}
      <rect x="48" y="162" width="314" height="14" fill="white" opacity="0.10"/>
    </g>
    <path d="{body_path}" fill="none" stroke="#111" stroke-width="1.4"/>

    <!-- lime-green LEFT side panel (robot's right, viewer's left in rear view), flush -->
    <path d="M 48 162 L -6 178 Q -16 184, -16 196 L -14 510 Q -14 522, -4 526 L 48 520 Q 58 516 60 508 L 60 168 Q 58 160 48 162 Z"
          fill="url(#{gg})" stroke="#4a8e1f" stroke-width="1"/>
    <rect x="-8" y="326" width="42" height="40" rx="5" fill="#c0d890" stroke="#7aae30" stroke-width="0.8"/>
    <rect x="-6" y="382" width="38" height="12" rx="3" fill="#a0c860"/>

    <!-- rear detail ports -->
    <circle cx="96"  cy="448" r="5"  fill="#1c2028"/>
    <circle cx="116" cy="448" r="5"  fill="#1c2028"/>
    <rect x="136" y="442" width="32" height="12" rx="2" fill="#1c2028"/>
    <!-- bottom strip -->
    <rect x="78" y="428" width="192" height="18" rx="3" fill="#e0e0e4" stroke="#a0a0a8" stroke-width="0.6"/>
    <circle cx="102" cy="437" r="5" fill="#1c2028"/>
    <circle cx="252" cy="437" r="5" fill="#1c2028"/>
  </g>"""


# ─────────────────────────────────────────────────────────────────────────────
# AUX SPEC PIECES — 71×48 covers + rear centre stack (1 user unit = 1 mm in groups below)
# ─────────────────────────────────────────────────────────────────────────────
def _cover_fill(sports, primary, secondary, dark, grad_id, w, h):
    """Branding fill for tiny cover artboards."""
    if sports:
        orad = min(88.0, max(w, h) * 0.5)
        rays = 18 if max(w, h) > 120 else 12
        return (
            f'<rect x="0" y="0" width="{w}" height="{h}" fill="url(#{grad_id})"/>'
            f"{sunburst_rays(w/2, h/2, 2, orad, rays, 6, secondary, 0.22)}"
        )
    return (
        f'<rect x="0" y="0" width="{w}" height="{h}" fill="url(#{grad_id})"/>'
        f'<rect x="0" y="0" width="{w}" height="{h}" fill="{secondary}" opacity="0.07"/>'
    )


def _aux_piece_fill(primary, secondary, dark, grad_id, w, h):
    """Clean fill for small spec add-ons — no sunburst (avoids stray ray lines at tiny sizes)."""
    return (
        f'<rect x="0" y="0" width="{w}" height="{h}" fill="url(#{grad_id})"/>'
        f'<rect x="0" y="0" width="{w}" height="{h}" fill="{secondary}" opacity="0.06"/>'
    )


def spec_charging_key_covers(
    x, y, display_scale, primary, secondary, text_hex, logo_data_url, eff_wrap,
    uid_suffix="a",
):
    """Two separate 71×48 mm artboards (physical charging + key covers on spec)."""
    dark = _darken(primary, 0.72)
    w, h = float(COVER_W), float(COVER_H)
    gap_mm = 14.0
    gid1 = f"cov1_{uid_suffix}"
    gid2 = f"cov2_{uid_suffix}"
    tiny_logo = ""
    if logo_data_url:
        tiny_logo = (
            f'<image href="{escape_xml(logo_data_url)}" x="{w/2-18}" y="{h/2-11}" width="36" height="22" '
            f'preserveAspectRatio="xMidYMid meet"/>'
        )
    c1, c2 = f"covclip1_{uid_suffix}", f"covclip2_{uid_suffix}"
    return f"""
  <g transform="translate({x},{y}) scale({display_scale})">
    <g>
      <defs>
        <clipPath id="{c1}"><rect x="0" y="0" width="{w}" height="{h}"/></clipPath>
        <clipPath id="{c2}"><rect x="0" y="0" width="{w}" height="{h}"/></clipPath>
        <radialGradient id="{gid1}" cx="50%" cy="55%" r="72%">
          <stop offset="0%"   stop-color="{primary}"/>
          <stop offset="70%"  stop-color="{primary}"/>
          <stop offset="100%" stop-color="{dark}"/>
        </radialGradient>
        <radialGradient id="{gid2}" cx="50%" cy="55%" r="72%">
          <stop offset="0%"   stop-color="{primary}"/>
          <stop offset="70%"  stop-color="{primary}"/>
          <stop offset="100%" stop-color="{dark}"/>
        </radialGradient>
      </defs>
    <g transform="translate(0,0)">
      <g clip-path="url(#{c1})">
        {_aux_piece_fill(primary, secondary, dark, gid1, w, h)}
        {tiny_logo}
      </g>
      <rect x="0" y="0" width="{w}" height="{h}" fill="none" stroke="#4a5568" stroke-width="0.85" rx="4"/>
    </g>
    <g transform="translate({w + gap_mm},0)">
      <g clip-path="url(#{c2})">
        {_aux_piece_fill(primary, secondary, dark, gid2, w, h)}
        {tiny_logo}
      </g>
      <rect x="0" y="0" width="{w}" height="{h}" fill="none" stroke="#4a5568" stroke-width="0.85" rx="4"/>
    </g>
    </g>
  </g>"""


def spec_rear_center_panels(
    x, y, display_scale, primary, secondary, text_hex, logo_data_url, eff_wrap,
    uid_suffix="a",
):
    """
    Three rear-centre interface pieces (mm). Bottom-aligned; rectangular placeholders
    (final die lines may be more complex on factory files).
    """
    dark = _darken(primary, 0.72)
    w1, h1 = REAR_CENTER_PIECE_A
    w2, h2 = REAR_CENTER_PIECE_B
    w3, h3 = REAR_CENTER_PIECE_C
    hmax = 229.5
    g12 = 10.0
    g23 = 10.0
    x1, y1 = 0.0, hmax - h1
    x2, y2 = w1 + g12, hmax - h2
    x3, y3 = w1 + g12 + w2 + g23, 0.0
    gid1, gid2, gid3 = f"rc1_{uid_suffix}", f"rc2_{uid_suffix}", f"rc3_{uid_suffix}"

    def one_piece(ox, oy, rw, rh, gidx):
        cid = f"rcclip_{gidx}"
        fill_b = _aux_piece_fill(primary, secondary, dark, gidx, float(rw), float(rh))
        lg = ""
        if logo_data_url and rh >= 100:
            lg = (
                f'<image href="{escape_xml(logo_data_url)}" x="{float(rw) / 2 - 50}" y="{float(rh) / 2 - 25}" '
                f'width="100" height="50" preserveAspectRatio="xMidYMid meet" opacity="0.82"/>'
            )
        return (
            f'<g transform="translate({ox},{oy})">'
            f'<defs>'
            f'<clipPath id="{cid}"><rect x="0" y="0" width="{rw}" height="{rh}"/></clipPath>'
            f'<radialGradient id="{gidx}" cx="50%" cy="55%" r="72%">'
            f'<stop offset="0%" stop-color="{primary}"/>'
            f'<stop offset="70%" stop-color="{primary}"/>'
            f'<stop offset="100%" stop-color="{dark}"/>'
            f"</radialGradient></defs>"
            f'<g clip-path="url(#{cid})">{fill_b}{lg}</g>'
            f'<rect x="0" y="0" width="{rw}" height="{rh}" fill="none" stroke="#4a5568" stroke-width="0.9" rx="4"/>'
            f"</g>"
        )

    g_inner = one_piece(x1, y1, w1, h1, gid1) + one_piece(x2, y2, w2, h2, gid2) + one_piece(x3, y3, w3, h3, gid3)
    return f"""
  <g transform="translate({x},{y}) scale({display_scale})">
    {g_inner}
  </g>"""


# ─────────────────────────────────────────────────────────────────────────────
# MAIN SVG BUILDER
# ─────────────────────────────────────────────────────────────────────────────
def build_cc1_svg(client_name, primary_hex, secondary_hex, text_hex,
                  extra_colours, extra_details, logo_data_url,
                  wrap_style="commercial", layout_preset="generic"):
    lp = (layout_preset or "generic").strip().lower()
    if lp not in VALID_LAYOUT_PRESETS:
        lp = "generic"
    eff_wrap = _wrap_for_preset(lp, wrap_style)
    partner_hex = _partner_hex_from_extras(extra_colours, secondary_hex)
    variant_line = escape_xml(VARIANT_BOARD_LABELS.get(lp, lp))
    _uid = _FILENAME_SAFE_RE.sub("_", (lp or "layout").strip()) or "layout"

    cn = escape_xml(client_name)
    details_text = (escape_xml(extra_details.strip())[:200]
                    if extra_details and extra_details.strip() else "")

    front_scale = 700 / FRONT_W      # ≈ 0.601
    back_scale  = 690 / BACK_W       # ≈ 0.660
    robot_scale = 0.80

    front_w_px = FRONT_W * front_scale
    front_h_px = FRONT_H * front_scale
    back_w_px  = BACK_W  * back_scale
    back_h_px  = BACK_H  * back_scale

    fx, fy = 30, 136
    bx, by = 30, fy + front_h_px + 72

    # Right column: stacked 3D previews (front above rear), aligned with flat artwork column
    flat_w_px = max(front_w_px, back_w_px)
    rfx = int(fx + flat_w_px + 42)
    # Local robot art ~0..666 tall; scaled footprint for vertical stacking gap
    _robot_local_h = 666.0
    robot_frame_h = _robot_local_h * robot_scale
    robot_stack_gap = 36.0
    ry_front = float(fy)
    ry_rear = ry_front + robot_frame_h + robot_stack_gap
    robot_label_cx = rfx + 230 * robot_scale

    # --- PUDU add-on 1:1 pieces (mm): 71×48 covers + three rear centre panels ---
    cover_disp_scale = 1.0
    rear_row_scale = 0.36
    aux_title_y = by + back_h_px + 20
    cov_y = aux_title_y + 22
    cov_caption_y = cov_y + COVER_H * cover_disp_scale + 8
    rear_y = cov_caption_y + 36
    rear_caption_y = rear_y + REAR_CENTER_PIECE_C[1] * rear_row_scale + 12
    # Footer: optional details — clear bottom of whichever column is taller (flat+aux vs robots)
    robot_column_bottom = ry_rear + robot_frame_h + 20
    details_y = max(rear_caption_y + 28, robot_column_bottom)
    W, H = 1700, int(details_y + 36)

    aux_covers = spec_charging_key_covers(
        fx, cov_y, cover_disp_scale,
        primary_hex, secondary_hex, text_hex, logo_data_url, eff_wrap, uid_suffix=_uid,
    )
    aux_rear = spec_rear_center_panels(
        fx, rear_y, rear_row_scale,
        primary_hex, secondary_hex, text_hex, logo_data_url, eff_wrap, uid_suffix=_uid,
    )

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink"
     width="{W}" height="{H}" viewBox="0 0 {W} {H}">
  <defs>
    <!-- Drop shadow: compatible feMerge approach -->
    <filter id="sh" x="-8%" y="-8%" width="116%" height="116%" color-interpolation-filters="sRGB">
      <feFlood flood-opacity="0.13" flood-color="#000" result="flood"/>
      <feComposite in="flood" in2="SourceGraphic" operator="in" result="shadow"/>
      <feGaussianBlur in="shadow" stdDeviation="7" result="blur"/>
      <feOffset dx="0" dy="5" result="offset"/>
      <feMerge><feMergeNode in="offset"/><feMergeNode in="SourceGraphic"/></feMerge>
    </filter>
  </defs>

  <!-- page background -->
  <rect width="100%" height="100%" fill="#f7f7f7"/>

  <!-- header -->
  <text x="{W/2}" y="48" text-anchor="middle" fill="#0d1320"
        font-family="Syne,Segoe UI,Arial,sans-serif" font-size="28" font-weight="800" letter-spacing="1">PUDU CC1 — Vinyl Wrap Mockup</text>
  <text x="{W/2}" y="72" text-anchor="middle" fill="#5c687d"
        font-family="DM Sans,Arial,sans-serif" font-size="16" letter-spacing="0.5">{cn}</text>
  <text x="{W/2}" y="92" text-anchor="middle" fill="#8b93a8"
        font-family="DM Sans,Arial,sans-serif" font-size="11" font-weight="600" letter-spacing="0.2">{variant_line}</text>
  <line x1="30" y1="102" x2="{W-30}" y2="102" stroke="#dde0e8" stroke-width="1"/>

  <!-- column labels -->
  <text x="{fx + front_w_px/2}" y="124" text-anchor="middle" fill="#8898b8"
        font-family="DM Sans,Arial,sans-serif" font-size="10" font-weight="700" letter-spacing="3">FLAT ARTWORK</text>
  <text x="{robot_label_cx}" y="124" text-anchor="middle" fill="#8898b8"
        font-family="DM Sans,Arial,sans-serif" font-size="10" font-weight="700" letter-spacing="3">3D PREVIEW</text>
  <text x="{robot_label_cx}" y="{ry_front - 6}" text-anchor="middle" fill="#a0a8b8"
        font-family="DM Sans,Arial,sans-serif" font-size="9" font-weight="600" letter-spacing="2">FRONT</text>
  <text x="{robot_label_cx}" y="{ry_rear - 6}" text-anchor="middle" fill="#a0a8b8"
        font-family="DM Sans,Arial,sans-serif" font-size="9" font-weight="600" letter-spacing="2">REAR</text>

  <!-- front shell flat artwork -->
  <g filter="url(#sh)">
    {front_decal(fx, fy, front_scale, primary_hex, secondary_hex, text_hex, logo_data_url, eff_wrap, lp, partner_hex)}
  </g>
  <text x="{fx + front_w_px/2}" y="{fy + front_h_px + 20}"
        text-anchor="middle" fill="#8898b8"
        font-family="DM Sans,Arial,sans-serif" font-size="10" font-weight="600" letter-spacing="2">FRONT SHELL · 1164 × 469 mm</text>

  <!-- back shell flat artwork -->
  <g filter="url(#sh)">
    {back_decal(bx, by, back_scale, primary_hex, secondary_hex, text_hex, logo_data_url, eff_wrap, lp, partner_hex)}
  </g>
  <text x="{bx + back_w_px/2}" y="{by + back_h_px + 22}"
        text-anchor="middle" fill="#8898b8"
        font-family="DM Sans,Arial,sans-serif" font-size="10" font-weight="600" letter-spacing="2">BACK SHELL · 1045 × 489 mm</text>

  <!-- PUDU CC1: extra physical stickers (1:1 sizes per factory sheet) -->
  <text x="{fx}" y="{aux_title_y}" fill="#0d1320" font-family="Syne,Segoe UI,Arial,sans-serif"
        font-size="12" font-weight="700" letter-spacing="2">ADDITIONAL STICKERS (1:1 mm)</text>
  <text x="{fx}" y="{aux_title_y + 14}" fill="#6c7588" font-family="DM Sans,Arial,sans-serif" font-size="9.5" font-style="italic">Charging + key covers (separate) · Rear centre interface stack (placeholders—confirm die lines in vendor file)</text>
  <g filter="url(#sh)">
    {aux_covers}
  </g>
  <text x="{fx + 78}" y="{cov_caption_y + 4}" text-anchor="middle" fill="#8898b8" font-family="DM Sans,Arial,sans-serif" font-size="8.5" font-weight="600">CHARGING + KEY (each 71×48 mm)</text>
  <g filter="url(#sh)">
    {aux_rear}
  </g>
  <text x="{fx + 160}" y="{rear_caption_y + 2}" text-anchor="middle" fill="#8898b8" font-family="DM Sans,Arial,sans-serif" font-size="8.5" font-weight="600"
        >REAR CENTRE: 293×143 · 275×110 · 303×229.5 mm</text>

  <!-- robot views: stacked vertically (front above rear) -->
  <g filter="url(#sh)">
    {robot_front(rfx, ry_front, robot_scale, primary_hex, secondary_hex, text_hex, logo_data_url, "f", eff_wrap, lp, partner_hex)}
  </g>
  <g filter="url(#sh)">
    {robot_rear(rfx, ry_rear, robot_scale, primary_hex, secondary_hex, text_hex, logo_data_url, "r", eff_wrap, lp, partner_hex)}
  </g>

  <text x="{W-40}" y="{details_y}" text-anchor="end" fill="#8898b8"
        font-family="DM Sans,Arial,sans-serif" font-size="10" font-style="italic">{details_text}</text>
</svg>"""
    return svg


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────
def generate_vinyl_wrap_spec_payload(
    client_name,
    primary_colour,
    secondary_colour,
    text_colour,
    extra_colours_json="[]",
    extra_details="",
    logo_bytes=None,
    logo_mime=None,
    wrap_style="commercial",
):
    cn = (client_name or "").strip()
    if not cn:
        raise ValueError("client_name is required")
    p = validate_hex(primary_colour, "primary_colour")
    s = validate_hex(secondary_colour, "secondary_colour")
    t = validate_hex(text_colour, "text_colour")
    extras = _parse_extra_colours(extra_colours_json)

    logo_data_url = None
    if logo_bytes is not None:
        if len(logo_bytes) == 0:
            raise ValueError("Logo file is empty")
        if len(logo_bytes) > MAX_LOGO_BYTES:
            raise ValueError(f"Logo exceeds maximum size ({MAX_LOGO_BYTES} bytes)")
        mime = (logo_mime or "application/octet-stream").split(";")[0].strip()
        b64 = base64.standard_b64encode(logo_bytes).decode("ascii")
        logo_data_url = f"data:{mime};base64,{b64}"

    ws = (wrap_style or "commercial").strip().lower()
    if ws not in ("commercial", "sports"):
        raise ValueError("wrap_style must be commercial or sports")

    safe_client = _sanitize_filename_part(cn)
    slug_for = {
        "generic": "layout-wings",
        "ics": "layout-centre-brand",
        "foodworks": "layout-retail-mirrored-rear",
        "sports_showcase": "layout-sports-partner",
    }
    variants = []
    for preset in LAYOUT_PRESETS:
        svg_text = build_cc1_svg(
            cn, p, s, t, extras, extra_details, logo_data_url,
            wrap_style=ws, layout_preset=preset,
        )
        raw_b64 = base64.standard_b64encode(svg_text.encode("utf-8")).decode("ascii")
        eff = _wrap_for_preset(preset, ws)
        variants.append({
            "id": preset,
            "label": VARIANT_BOARD_LABELS.get(preset, preset),
            "layout_preset": preset,
            "effective_wrap_style": eff,
            "svg_text": svg_text,
            "image_base64": raw_b64,
            "image_mime": "image/svg+xml",
            "filename": f"{safe_client} - Vinyl Wrap - {slug_for.get(preset, preset)}.svg",
        })

    primary_variant = variants[0]
    return {
        "success": True,
        "variants": variants,
        "svg_text": primary_variant["svg_text"],
        "image_base64": primary_variant["image_base64"],
        "image_mime": "image/svg+xml",
        "filename": primary_variant["filename"],
    }


# ── quick test ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    style = sys.argv[1] if len(sys.argv) > 1 else "commercial"
    result = generate_vinyl_wrap_spec_payload(
        client_name="Epworth Hospital",
        primary_colour="#1B4F9B",
        secondary_colour="#5AADDB",
        text_colour="#FFFFFF",
        wrap_style=style,
    )
    out = f"/home/user/workspace/test_output_{style}.svg"
    with open(out, "w", encoding="utf-8") as f:
        f.write(result["svg_text"])
    print(f"Written: {out}")