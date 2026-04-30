import asyncio
import hashlib
import io
import os
import re
import struct
import zlib
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
import oss2
from openai import AsyncOpenAI
from PIL import Image
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from env_loader import get_dashscope_api_key, load_crawl_env
from url_utils import normalize_article_url


load_crawl_env()

OSS_ACCESS_KEY_ID = os.getenv("OSS_ACCESS_KEY_ID", "").strip()
OSS_ACCESS_KEY_SECRET = os.getenv("OSS_ACCESS_KEY_SECRET", "").strip()
OSS_BUCKET_NAME = os.getenv("OSS_BUCKET_NAME", "").strip()
OSS_ENDPOINT = os.getenv("OSS_ENDPOINT", "").strip()
OSS_DOMAIN = os.getenv("OSS_DOMAIN", "").strip().rstrip("/")

DS_API_KEY = get_dashscope_api_key()
DASHSCOPE_TASKS_URL = "https://dashscope.aliyuncs.com/api/v1/tasks"
DASHSCOPE_T2I_URL = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text2image/image-synthesis"
DS_COMPAT_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DS_PROMPT_MODEL = os.getenv("DASHSCOPE_PROMPT_MODEL", "").strip() or "qwen-plus"
DEFAULT_T2I_MODEL = os.getenv("DASHSCOPE_T2I_MODEL", "").strip() or "wan2.5-t2i-preview"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
META_PATTERNS = [
    re.compile(
        r'<meta[^>]+property=["\']og:image(?::secure_url)?["\'][^>]+content=["\']([^"\']+)["\']',
        re.I,
    ),
    re.compile(
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image(?::secure_url)?["\']',
        re.I,
    ),
    re.compile(
        r'<meta[^>]+name=["\']twitter:image(?::src)?["\'][^>]+content=["\']([^"\']+)["\']',
        re.I,
    ),
    re.compile(
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']twitter:image(?::src)?["\']',
        re.I,
    ),
]
ICON_PATTERNS = [
    re.compile(r'<link[^>]+rel=["\'][^"\']*apple-touch-icon[^"\']*["\'][^>]+href=["\']([^"\']+)["\']', re.I),
    re.compile(r'<link[^>]+rel=["\'][^"\']*icon[^"\']*["\'][^>]+href=["\']([^"\']+)["\']', re.I),
]
_bucket: Optional[oss2.Bucket] = None
_llm_client: Optional[AsyncOpenAI] = None


def _oss_enabled() -> bool:
    return all([OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_BUCKET_NAME, OSS_ENDPOINT, OSS_DOMAIN])


def _get_bucket() -> oss2.Bucket:
    global _bucket
    if _bucket is None:
        auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
        _bucket = oss2.Bucket(auth, f"https://{OSS_ENDPOINT}", OSS_BUCKET_NAME)
    return _bucket


def _get_llm_client() -> AsyncOpenAI:
    global _llm_client
    if _llm_client is None:
        _llm_client = AsyncOpenAI(api_key=DS_API_KEY, base_url=DS_COMPAT_BASE_URL)
    return _llm_client


def _make_object_key(article_url: str, ext: str) -> str:
    stable_url = normalize_article_url(article_url) or (article_url or "").strip()
    digest = hashlib.sha1(stable_url.encode("utf-8")).hexdigest()
    return f"news-covers/{digest}{ext}"


def _guess_ext(content_type: str, fallback_url: str = "") -> str:
    ctype = (content_type or "").lower()
    if "png" in ctype:
        return ".png"
    if "webp" in ctype:
        return ".webp"
    if "gif" in ctype:
        return ".gif"
    if "jpeg" in ctype or "jpg" in ctype:
        return ".jpg"
    path = urlparse(fallback_url).path.lower()
    for ext in (".png", ".webp", ".gif", ".jpg", ".jpeg"):
        if path.endswith(ext):
            return ".jpg" if ext == ".jpeg" else ext
    return ".jpg"


def _compress_image_bytes(
    raw: bytes, max_width: int = 960, quality: int = 82
) -> Tuple[bytes, str, str]:
    """
    将任意图片压缩为 JPEG（缩放 + 质量压缩），返回 (data, ext, content_type)。
    目标：单张缩略图 < 300KB，大幅减少前端加载耗时。
    """
    try:
        img = Image.open(io.BytesIO(raw))
        if img.mode in ("RGBA", "P", "LA"):
            img = img.convert("RGB")
        elif img.mode != "RGB":
            img = img.convert("RGB")
        w, h = img.size
        if w > max_width:
            ratio = max_width / w
            img = img.resize((max_width, int(h * ratio)), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=True)
        return buf.getvalue(), ".jpg", "image/jpeg"
    except Exception:
        return raw, ".png", "image/png"


def _is_probably_blank_png(png_bytes: bytes) -> bool:
    """
    轻量级空白图检测（不依赖 Pillow）：
    - 解析 PNG IHDR + IDAT，解压少量 scanline 样本
    - 若像素整体几乎纯黑/纯白且方差极低，判为“空白/骨架屏占位”
    只用于兜底流程：误判最多导致多走一次 og/t2i。
    """
    if not png_bytes or not png_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        return False
    try:
        o = 8
        width = height = bit_depth = color_type = None
        idat = b""
        while o + 12 <= len(png_bytes):
            ln = struct.unpack(">I", png_bytes[o : o + 4])[0]
            o += 4
            typ = png_bytes[o : o + 4]
            o += 4
            dat = png_bytes[o : o + ln]
            o += ln
            o += 4  # crc
            if typ == b"IHDR":
                width, height, bit_depth, color_type = struct.unpack(">IIBB", dat[:10])
            elif typ == b"IDAT":
                idat += dat
            elif typ == b"IEND":
                break

        if not width or not height or bit_depth != 8 or color_type not in (2, 6) or not idat:
            return False

        raw = zlib.decompress(idat)
        bpp = 3 if color_type == 2 else 4
        stride = 1 + width * bpp
        if len(raw) < stride:
            return True

        # 采样前 N 行、每行前 M 字节，估算均值/方差
        sample = bytearray()
        max_rows = min(height, 6)
        max_bytes_per_row = min(width * bpp, 3600)
        for y in range(max_rows):
            row = raw[y * stride : (y + 1) * stride]
            if not row:
                continue
            sample.extend(row[1 : 1 + max_bytes_per_row])  # skip filter byte

        if not sample:
            return True

        mean = sum(sample) / len(sample)
        var = sum((x - mean) ** 2 for x in sample) / len(sample)

        # 经验阈值：空白/极暗/极亮页通常 var 很低，mean 接近 0 或 255
        if var < 120 and (mean < 8 or mean > 247):
            return True
        return False
    except Exception:
        # 解析失败不做空白判定，避免误伤
        return False


def _normalize_url(url: str, base_url: str) -> Optional[str]:
    candidate = (url or "").strip()
    if not candidate:
        return None
    if candidate.startswith("//"):
        return f"{urlparse(base_url).scheme}:{candidate}"
    if candidate.startswith("data:"):
        return None
    try:
        return urljoin(base_url, candidate)
    except Exception:
        return None


async def _extract_html_cover_url(client: httpx.AsyncClient, article_url: str) -> Optional[str]:
    resp = await client.get(article_url, follow_redirects=True)
    resp.raise_for_status()
    html = resp.text or ""
    final_url = str(resp.url)
    for pattern in META_PATTERNS:
        match = pattern.search(html)
        if match:
            candidate = _normalize_url(match.group(1), final_url)
            if candidate:
                return candidate
    for pattern in ICON_PATTERNS:
        match = pattern.search(html)
        if match:
            candidate = _normalize_url(match.group(1), final_url)
            if candidate:
                return candidate
    parsed = urlparse(final_url)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}/favicon.ico"
    return None


async def _download_image_bytes(client: httpx.AsyncClient, image_url: str) -> Optional[Tuple[bytes, str]]:
    resp = await client.get(image_url, follow_redirects=True)
    resp.raise_for_status()
    content_type = resp.headers.get("content-type", "")
    if not content_type.startswith("image/"):
        return None
    data = resp.content
    if not data:
        return None
    return data, content_type


def _dashscope_enabled() -> bool:
    return bool(DS_API_KEY)


def _build_t2i_prompt(title: str) -> str:
    clean = (title or "").strip()
    base = (
        "电影感横版封面图，高细节，高质感，适配深色 UI 缩略图。"
        "画面中不要出现任何文字、水印、Logo、边框、UI 截图。"
        "主体清晰，背景有层次与氛围光。"
    )
    if not clean:
        return base
    return f"{base}\n主题：{clean}"


async def _build_t2i_prompt_via_llm(title: str) -> str:
    """
    参考 generatePicture 的思路：先让 qwen-plus 把标题扩写成更具体的文生图提示词，
    再交给万相出图，减少不同文章出现“同质图”。
    """
    fallback = _build_t2i_prompt(title)
    if not _dashscope_enabled():
        return fallback
    clean_title = (title or "").strip()
    if not clean_title:
        return fallback
    prompt = (
        "你是视觉设计助理。请把给定新闻标题改写为一段中文文生图提示词（80-180字）。\n"
        "要求：\n"
        "1) 电影感、信息密度高、细节具体；\n"
        "2) 强相关于标题语义，不要泛化成通用AI海报；\n"
        "3) 不要出现任何文字、Logo、水印、边框、截图UI；\n"
        "4) 适配深色UI卡片缩略图；\n"
        "5) 只输出提示词正文，不要解释。\n\n"
        f"标题：{clean_title}"
    )
    try:
        client = _get_llm_client()
        resp = await client.chat.completions.create(
            model=DS_PROMPT_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
        )
        text = (resp.choices[0].message.content or "").strip()
        if text:
            return text[:1200]
    except Exception as exc:
        print(f"⚠️ t2i prompt rewrite failed: title={clean_title[:40]} err={exc}")
    return fallback


async def _dashscope_create_t2i_task(client: httpx.AsyncClient, prompt: str) -> Optional[str]:
    headers = {
        "Authorization": f"Bearer {DS_API_KEY}",
        "Content-Type": "application/json",
        "X-DashScope-Async": "enable",
    }
    body = {
        "model": DEFAULT_T2I_MODEL,
        "input": {"prompt": prompt},
        "parameters": {
            # 与 generatePicture 保持一致，减少模型因分辨率导致的异常退化
            "size": "1280*1280",
            "n": 1,
            "prompt_extend": True,
            "watermark": False,
        },
    }
    resp = await client.post(DASHSCOPE_T2I_URL, headers=headers, json=body)
    resp.raise_for_status()
    data = resp.json() or {}
    if data.get("code"):
        raise RuntimeError(f"dashscope t2i create failed: {data.get('code')} {data.get('message')}")
    task_id = (data.get("output") or {}).get("task_id")
    return str(task_id).strip() if task_id else None


async def _dashscope_poll_t2i_result_url(client: httpx.AsyncClient, task_id: str) -> Optional[str]:
    headers = {"Authorization": f"Bearer {DS_API_KEY}"}
    url = f"{DASHSCOPE_TASKS_URL}/{task_id}"
    # 最多等 90s：封面兜底不宜阻塞太久
    for _ in range(30):
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json() or {}
        out = data.get("output") or {}
        status = out.get("task_status")
        if status == "SUCCEEDED":
            results = out.get("results") or []
            for item in results:
                u = (item or {}).get("url")
                if u:
                    return str(u).strip()
            return None
        if status == "FAILED":
            raise RuntimeError(f"dashscope t2i failed: {out.get('code')} {out.get('message')}")
        await asyncio.sleep(3.0)
    return None


async def _generate_ai_cover_bytes(title: str) -> Optional[bytes]:
    # Temporary kill switch: disable text-to-image generation entirely.
    return None
    if not _dashscope_enabled():
        return None
    prompt = await _build_t2i_prompt_via_llm(title)
    timeout = httpx.Timeout(60.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            task_id = await _dashscope_create_t2i_task(client, prompt)
            if not task_id:
                return None
            result_url = await _dashscope_poll_t2i_result_url(client, task_id)
            if not result_url:
                return None
            img = await client.get(result_url, follow_redirects=True)
            img.raise_for_status()
            return img.content or None
        except Exception as exc:
            print(f"⚠️ t2i cover failed: title={title[:40]} err={exc}")
            return None


async def _capture_screenshot(article_url: str) -> Optional[bytes]:
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page(viewport={"width": 1280, "height": 720})
            # 更稳的截图策略：先保证 DOM ready，再尽量等网络/渲染稳定，避免截到骨架屏
            await page.goto(article_url, wait_until="domcontentloaded", timeout=20000)
            try:
                await page.wait_for_load_state("networkidle", timeout=12000)
            except Exception:
                pass
            try:
                await page.wait_for_selector("main, article, [role='main']", timeout=8000)
            except Exception:
                pass
            try:
                await page.wait_for_function(
                    """
                    () => {
                      const el = document.querySelector('main, article, [role="main"]') || document.body;
                      if (!el) return false;
                      const textLen = (el.innerText || '').replace(/\\s+/g,' ').trim().length;
                      const imgs = Array.from(document.images || []);
                      const loadedImgs = imgs.filter(img => img && img.complete && img.naturalWidth > 64 && img.naturalHeight > 64).length;
                      return textLen >= 120 || loadedImgs >= 1;
                    }
                    """,
                    timeout=8000,
                )
            except Exception:
                pass
            await page.wait_for_timeout(800)
            png = await page.screenshot(type="png")
            await browser.close()
            return png
    except PlaywrightTimeoutError:
        print(f"⚠️ cover screenshot timeout: url={article_url}")
        return None
    except Exception as exc:
        print(f"⚠️ cover screenshot failed: url={article_url} err={exc}")
        return None


async def _upload_bytes(article_url: str, payload: bytes, ext: str, content_type: str) -> Optional[str]:
    if not _oss_enabled():
        return None
    object_key = _make_object_key(article_url, ext)

    def _do_upload() -> str:
        bucket = _get_bucket()
        bucket.put_object(
            object_key,
            payload,
            headers={
                "Content-Type": content_type,
                "Cache-Control": "public, max-age=31536000, immutable",
            },
        )
        return f"{OSS_DOMAIN}/{object_key}"

    try:
        return await asyncio.to_thread(_do_upload)
    except Exception as exc:
        print(f"⚠️ OSS upload failed: url={article_url} err={exc}")
        return None


async def fetch_and_upload_cover(article_url: str, title: str = "") -> Optional[str]:
    target_url = normalize_article_url(article_url)
    if not target_url or not _oss_enabled():
        return None
    host = (urlparse(target_url).hostname or "").lower()
    is_openai_domain = host == "openai.com" or host.endswith(".openai.com")

    # OpenAI 域名直接文生图，避免无头渲染骨架/空白页
    if is_openai_domain:
        return None

    timeout = httpx.Timeout(15.0, connect=8.0)
    headers = {"User-Agent": USER_AGENT}
    async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
        try:
            image_url = await _extract_html_cover_url(client, target_url)
            if image_url:
                downloaded = await _download_image_bytes(client, image_url)
                if downloaded:
                    raw, _ct = downloaded
                    data, ext, ct = _compress_image_bytes(raw)
                    uploaded_url = await _upload_bytes(target_url, data, ext, ct)
                    if uploaded_url:
                        return uploaded_url
        except Exception as exc:
            print(f"⚠️ og-image fetch failed: url={target_url} err={exc}")

    screenshot = await _capture_screenshot(target_url)
    if screenshot:
        if _is_probably_blank_png(screenshot):
            screenshot = None
        else:
            data, ext, ct = _compress_image_bytes(screenshot)
            uploaded = await _upload_bytes(target_url, data, ext, ct)
            if uploaded:
                return uploaded

    # 最后一层兜底：文生图（标题 → 图），再上传 OSS
    return None


async def enrich_articles_with_covers(articles: List[Dict], concurrency: int = 6) -> List[Dict]:
    if not articles or not _oss_enabled():
        return articles
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def _worker(article: Dict) -> None:
        if article.get("cover_url"):
            return
        article_url = str(article.get("url", "") or "").strip()
        if not article_url:
            return
        title = str(article.get("title", "") or "").strip()
        async with semaphore:
            cover_url = await fetch_and_upload_cover(article_url, title=title)
            if cover_url:
                article["cover_url"] = cover_url

    await asyncio.gather(*[_worker(article) for article in articles])
    success_count = sum(1 for article in articles if article.get("cover_url"))
    print(f"🖼️ 封面上传完成: {success_count}/{len(articles)}")
    return articles


async def enrich_articles_with_ai_covers(articles: List[Dict], concurrency: int = 3) -> List[Dict]:
    """
    最后一层显式兜底：仅对仍无 cover_url 的文章，按标题文生图并上传 OSS。
    用于在爬虫执行顺序末尾统一补齐，避免前序策略漏网。
    """
    if not articles or not _oss_enabled() or not _dashscope_enabled():
        return articles
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def _worker(article: Dict) -> None:
        if article.get("cover_url"):
            return
        article_url = str(article.get("url", "") or "").strip()
        if not article_url:
            return
        title = str(article.get("title", "") or "").strip()
        async with semaphore:
            ai_img = await _generate_ai_cover_bytes(title)
            if not ai_img:
                return
            cover_url = await _upload_bytes(article_url, ai_img, ".png", "image/png")
            if cover_url:
                article["cover_url"] = cover_url

    await asyncio.gather(*[_worker(article) for article in articles])
    success_count = sum(1 for article in articles if article.get("cover_url"))
    print(f"🎨 文生图兜底完成: {success_count}/{len(articles)}")
    return articles
