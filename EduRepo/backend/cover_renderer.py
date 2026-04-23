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

    # Windows common fonts (demo machine is Windows)
    candidates = [
        r"C:\Windows\Fonts\msyhbd.ttc",
        r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
        r"C:\Windows\Fonts\simhei.ttf",
        r"C:\Windows\Fonts\simsun.ttc",
    ]
    for p in candidates:
        if os.path.exists(p):
            return p

    # Linux common fonts (for container later)
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


def _rounded_rect(img: Image.Image, xy: Tuple[int, int, int, int], radius: int, fill: Tuple[int, int, int, int]):
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle(xy, radius=radius, fill=fill)


def _gradient_bg(spec: CoverSpec, c1: Tuple[int, int, int], c2: Tuple[int, int, int]) -> Image.Image:
    w, h = spec.width, spec.height
    base = Image.new("RGB", (w, h), c1)
    top = Image.new("RGB", (w, h), c2)
    mask = Image.new("L", (w, h))
    md = ImageDraw.Draw(mask)
    for y in range(h):
        md.line((0, y, w, y), fill=int(255 * (y / max(1, h - 1))))
    return Image.composite(top, base, mask)


def _grid_overlay(img: Image.Image, step: int = 48, alpha: int = 26):
    w, h = img.size
    overlay = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    d = ImageDraw.Draw(overlay)
    color = (255, 255, 255, alpha)
    for x in range(0, w, step):
        d.line((x, 0, x, h), fill=color, width=1)
    for y in range(0, h, step):
        d.line((0, y, w, y), fill=color, width=1)
    img.alpha_composite(overlay)


def render_cover_png(
    template_id: str,
    title: str,
    highlights: List[str] | None = None,
    size: str = "3x4",
) -> bytes:
    spec = SIZES.get(size, SIZES["3x4"])
    tid = (template_id or "t1").strip().lower()
    hl = [h for h in (highlights or []) if h][:3]
    title = (title or "").strip()[:60]

    if tid == "t2":
        img = Image.new("RGBA", (spec.width, spec.height), (250, 244, 235, 255))
        # paper shadow
        shadow = Image.new("RGBA", img.size, (0, 0, 0, 0))
        _rounded_rect(shadow, (46, 56, spec.width - 26, spec.height - 26), 40, (0, 0, 0, 60))
        shadow = shadow.filter(ImageFilter.GaussianBlur(10))
        img.alpha_composite(shadow)
        _rounded_rect(img, (30, 40, spec.width - 40, spec.height - 40), 36, (255, 255, 255, 235))
    elif tid == "t3":
        img = _gradient_bg(spec, (7, 18, 38), (12, 30, 70)).convert("RGBA")
        _grid_overlay(img, step=52, alpha=28)
    elif tid == "t4":
        img = Image.new("RGBA", (spec.width, spec.height), (255, 214, 68, 255))
        burst = Image.new("RGBA", img.size, (0, 0, 0, 0))
        bd = ImageDraw.Draw(burst)
        cx, cy = spec.width // 2, int(spec.height * 0.42)
        for i in range(32):
            ang = i * (360 / 32)
            r1 = int(min(spec.width, spec.height) * 0.16)
            r2 = int(min(spec.width, spec.height) * 0.46)
            x1 = cx + int(r1 * __import__("math").cos(__import__("math").radians(ang)))
            y1 = cy + int(r1 * __import__("math").sin(__import__("math").radians(ang)))
            x2 = cx + int(r2 * __import__("math").cos(__import__("math").radians(ang)))
            y2 = cy + int(r2 * __import__("math").sin(__import__("math").radians(ang)))
            bd.line((x1, y1, x2, y2), fill=(255, 255, 255, 130), width=10)
        burst = burst.filter(ImageFilter.GaussianBlur(0.6))
        img.alpha_composite(burst)
    else:
        img = _gradient_bg(spec, (128, 90, 255), (255, 92, 162)).convert("RGBA")

    draw = ImageDraw.Draw(img)

    # header chip
    chip_text = "AI 新概念"
    chip_font = _font(32)
    chip_w = int(draw.textlength(chip_text, font=chip_font) + 34)
    chip_h = 54
    chip_x, chip_y = 46, 52
    chip_bg = (0, 0, 0, 70) if tid in ("t1", "t3") else (0, 0, 0, 35)
    _rounded_rect(img, (chip_x, chip_y, chip_x + chip_w, chip_y + chip_h), 20, chip_bg)
    draw.text((chip_x + 18, chip_y + 10), chip_text, font=chip_font, fill=(255, 255, 255, 240))

    # title
    pad_x = 62
    top = int(spec.height * 0.18)
    max_w = spec.width - pad_x * 2
    title_font = _font(72 if spec.width <= 900 else 78)
    lines = _wrap(draw, title, title_font, max_w=max_w)
    lines = lines[:4]

    y = top
    for line in lines:
        # highlight background for first matching word
        hl_color = (255, 245, 80, 190) if tid in ("t2", "t4") else (0, 0, 0, 55)
        if hl:
            for w in hl:
                if w and w in line:
                    x = pad_x
                    wlen = draw.textlength(line, font=title_font)
                    _rounded_rect(img, (x - 14, y - 10, x + int(wlen) + 18, y + 92), 24, hl_color)
                    break

        fill = (255, 255, 255, 245) if tid in ("t1", "t3") else (22, 24, 28, 245)
        stroke = (0, 0, 0, 190) if tid in ("t1", "t3") else (255, 255, 255, 220)
        if tid == "t4":
            fill = (20, 20, 20, 255)
            stroke = (255, 255, 255, 240)

        # shadow for more "art" feel
        shadow_offset = 6 if tid != "t2" else 2
        draw.text(
            (pad_x + shadow_offset, y + shadow_offset),
            line,
            font=title_font,
            fill=(0, 0, 0, 110),
            stroke_width=0,
        )
        draw.text(
            (pad_x, y),
            line,
            font=title_font,
            fill=fill,
            stroke_width=6 if tid in ("t1", "t3", "t4") else 2,
            stroke_fill=stroke,
        )
        y += 96

    # badges
    badge_font = _font(30)
    bx, by = 62, spec.height - 148
    badges = hl if hl else ["入门", "科普"]
    for b in badges[:3]:
        t = str(b).strip()
        if not t:
            continue
        tw = int(draw.textlength(t, font=badge_font) + 30)
        _rounded_rect(img, (bx, by, bx + tw, by + 50), 18, (0, 0, 0, 65))
        draw.text((bx + 16, by + 10), t, font=badge_font, fill=(255, 255, 255, 235))
        bx += tw + 14

    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="PNG", optimize=True)
    return buf.getvalue()

