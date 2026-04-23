from __future__ import annotations

import io
import os
import re
from dataclasses import dataclass
from typing import List, Tuple

from PIL import Image, ImageDraw, ImageFont, ImageFilter


@dataclass(frozen=True)
class CoverSpec:
    width: int
    height: int


SIZES: dict[str, CoverSpec] = {
    "3x4": CoverSpec(900, 1200),
    "1x1": CoverSpec(1080, 1080),
    "9x16": CoverSpec(900, 1600),
}


def _find_font_path() -> str | None:
    explicit = str(os.getenv("EDU_FONT_PATH", "") or "").strip()
    if explicit and os.path.exists(explicit):
        return explicit

    # Windows common fonts
    candidates = [
        r"C:\Windows\Fonts\msyhbd.ttc",
        r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
        r"C:\Windows\Fonts\simsun.ttc",
    ]
    for p in candidates:
        if os.path.exists(p):
            return p

    # Linux common fonts
    candidates = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return None


def _font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    path = _find_font_path()
    if not path:
        return ImageFont.load_default()
    try:
        return ImageFont.truetype(path, size=size)
    except Exception:
        return ImageFont.load_default()


def _rounded_rect(draw: ImageDraw.ImageDraw, xy: Tuple[int, int, int, int], radius: int, fill, outline=None, width=1):
    draw.rounded_rectangle(xy, radius=radius, fill=fill, outline=outline, width=width)


def _wrap(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_w: int) -> List[str]:
    text = re.sub(r"\s+", " ", (text or "").strip())
    if not text:
        return [""]
    lines: list[str] = []
    buf = ""
    for ch in text:
        trial = (buf + ch).strip()
        w = draw.textlength(trial, font=font)
        if w <= max_w or not buf:
            buf = trial
            continue
        lines.append(buf)
        buf = ch.strip()
    if buf:
        lines.append(buf)
    return lines


def _draw_highlight_line(
    img: Image.Image,
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    line: str,
    font: ImageFont.ImageFont,
    highlights: List[str],
    style: str,
):
    """
    Draw a title line with optional highlight background for matched keywords.
    """
    if highlights:
        for w in highlights:
            if w and w in line:
                tw = int(draw.textlength(line, font=font))
                if style == "pink":
                    # small cyan highlight block like screenshot
                    bg = (120, 230, 250, 170)
                elif style == "note":
                    bg = (146, 248, 177, 180)
                else:
                    bg = (0, 0, 0, 35)
                _rounded_rect(draw, (x - 10, y - 8, x + tw + 14, y + int(font.size * 1.25)), 18, bg)
                break

    # shadow + text
    shadow = (0, 0, 0, 55)
    draw.text((x + 3, y + 3), line, font=font, fill=shadow)
    draw.text((x, y), line, font=font, fill=(30, 30, 30, 255))


def _template_pink_quote(spec: CoverSpec, title: str, highlights: List[str]) -> Image.Image:
    img = Image.new("RGBA", (spec.width, spec.height), (255, 255, 255, 255))
    draw = ImageDraw.Draw(img)

    pad = 56
    # card
    card = (pad, pad, spec.width - pad, spec.height - pad)
    _rounded_rect(draw, card, 44, (249, 220, 229, 255))
    # quote icon
    q_font = _font(120)
    draw.text((pad + 50, pad + 36), "“", font=q_font, fill=(242, 175, 196, 160))

    # title
    title_font = _font(64)
    x = pad + 92
    y = pad + 290
    max_w = spec.width - x - pad - 40
    lines = _wrap(draw, title, title_font, max_w=max_w)[:4]
    for line in lines:
        _draw_highlight_line(img, draw, x, y, line, title_font, highlights, style="pink")
        y += 96

    # small badge (like sos)
    badge_text = "科普"
    b_font = _font(32)
    tw = int(draw.textlength(badge_text, font=b_font))
    bx, by = pad + 92, spec.height - pad - 130
    _rounded_rect(draw, (bx, by, bx + tw + 36, by + 56), 18, (255, 92, 122, 240))
    draw.text((bx + 18, by + 12), badge_text, font=b_font, fill=(255, 255, 255, 255))
    return img


def _template_blue_note(spec: CoverSpec, title: str, highlights: List[str]) -> Image.Image:
    img = Image.new("RGBA", (spec.width, spec.height), (255, 255, 255, 255))
    draw = ImageDraw.Draw(img)

    # blue frame
    _rounded_rect(draw, (36, 36, spec.width - 36, spec.height - 36), 54, (58, 116, 255, 255))

    # inner paper with slight shadow
    paper = Image.new("RGBA", (spec.width, spec.height), (0, 0, 0, 0))
    pd = ImageDraw.Draw(paper)
    _rounded_rect(pd, (86, 82, spec.width - 86, spec.height - 86), 40, (0, 0, 0, 70))
    paper = paper.filter(ImageFilter.GaussianBlur(10))
    img.alpha_composite(paper)

    _rounded_rect(draw, (70, 66, spec.width - 70, spec.height - 90), 40, (250, 252, 255, 255))

    # top bits
    small_font = _font(22)
    draw.text((102, 92), "⋯", font=_font(40), fill=(40, 60, 90, 200))
    draw.text((spec.width - 220, 96), "Text Note", font=small_font, fill=(40, 90, 190, 200))

    # title
    title_font = _font(72)
    x = 110
    y = 260
    max_w = spec.width - x - 110
    lines = _wrap(draw, title, title_font, max_w=max_w)[:4]
    for line in lines:
        _draw_highlight_line(img, draw, x, y, line, title_font, highlights, style="note")
        y += 106

    # underline line near bottom
    draw.line((110, spec.height - 220, spec.width - 110, spec.height - 220), fill=(80, 120, 180, 60), width=3)
    return img


def _template_clean(spec: CoverSpec, title: str, highlights: List[str]) -> Image.Image:
    img = Image.new("RGBA", (spec.width, spec.height), (255, 255, 255, 255))
    draw = ImageDraw.Draw(img)
    _rounded_rect(draw, (46, 46, spec.width - 46, spec.height - 46), 42, (252, 252, 252, 255), outline=(235, 238, 245, 255), width=2)

    # subtle corner marks
    draw.rectangle((72, 72, 112, 80), fill=(255, 80, 140, 190))
    draw.rectangle((spec.width - 120, spec.height - 86, spec.width - 80, spec.height - 78), fill=(110, 225, 250, 190))

    title_font = _font(68)
    x = 86
    y = 240
    max_w = spec.width - x - 86
    lines = _wrap(draw, title, title_font, max_w=max_w)[:4]
    for line in lines:
        _draw_highlight_line(img, draw, x, y, line, title_font, highlights, style="clean")
        y += 100
    return img


def render_cover_png(template_id: str, title: str, highlights: List[str] | None = None, size: str = "3x4") -> bytes:
    spec = SIZES.get(size, SIZES["3x4"])
    tid = (template_id or "t1").strip().lower()
    hl = [h for h in (highlights or []) if h][:3]
    title = (title or "").strip()[:60]
    if not title:
        title = "3分钟搞懂：AI 新概念"

    if tid in ("t2", "pink", "quote"):
        img = _template_pink_quote(spec, title, hl)
    elif tid in ("t1", "note", "blue"):
        img = _template_blue_note(spec, title, hl)
    elif tid in ("t3", "clean"):
        img = _template_clean(spec, title, hl)
    else:
        # fallback: reuse clean
        img = _template_clean(spec, title, hl)

    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="PNG", optimize=True)
    return buf.getvalue()

