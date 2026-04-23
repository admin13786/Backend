#!/usr/bin/env python3
"""
根据「创意精选 / 技能提升」视频榜每条标题（默认各前 N 条，与 rank.js 中 VIDEO_RANK_SIZE 一致）：
1) 用百炼 OpenAI 兼容接口（qwen-plus）把标题改写成文生图描述；
2) 用百炼原生文生图异步 API（万相）出图；
3) 写出 creative-rank-{id}.png / skill-rank-{id}.png，并复制到 FrontEnd/public/channel-covers/；
4) 可选：--composite 仅支持 --max-rank 1（单卡贴图调试）。

使用前请设置环境变量: export DASHSCOPE_API_KEY=sk-xxx

文生图走异步 HTTP（官方文档推荐），因 compatible-mode/v1 当前主要覆盖对话类模型。
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path

import requests
from openai import OpenAI
from PIL import Image

DASHSCOPE_TASKS_URL = "https://dashscope.aliyuncs.com/api/v1/tasks"
DASHSCOPE_T2I_URL = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text2image/image-synthesis"
# 文档示例模型；若在控制台未开通可改为账户已开通的万相文生图模型名
DEFAULT_T2I_MODEL = "wan2.5-t2i-preview"


def extract_padvideo_inner(rank_js: Path, const_name: str, series_label: str) -> str:
    text = rank_js.read_text(encoding="utf-8")
    m = re.search(
        rf"const\s+{re.escape(const_name)}\s*=\s*padVideoList\(\s*\[(.*?)\]\s*,\s*\{{\s*series:\s*'{re.escape(series_label)}'",
        text,
        re.DOTALL,
    )
    if not m:
        raise ValueError(f"无法解析 {const_name}（series: {series_label}）")
    return m.group(1)


def parse_video_id_titles(inner: str) -> list[tuple[int, str]]:
    pairs: list[tuple[int, str]] = []
    for m in re.finditer(r"\{\s*id:\s*(\d+)\s*,\s*title:\s*'([^']*)'", inner):
        pairs.append((int(m.group(1)), m.group(2)))
    pairs.sort(key=lambda x: x[0])
    return pairs


def load_rank_jobs(
    rank_js: Path, min_rank: int, max_rank: int, only: str
) -> list[tuple[str, str, int, str]]:
    """(basename_prefix, section_label, rank_id, title) — basename 如 creative-rank-3"""
    jobs: list[tuple[str, str, int, str]] = []
    if only in ("creative", "both"):
        inner = extract_padvideo_inner(rank_js, "MOCK_MAIN_VIDEO", "创意精选")
        for rid, title in parse_video_id_titles(inner):
            if min_rank <= rid <= max_rank:
                jobs.append((f"creative-rank-{rid}", "创意精选", rid, title))
    if only in ("skill", "both"):
        inner = extract_padvideo_inner(rank_js, "MOCK_SUB_VIDEO", "技能提升")
        for rid, title in parse_video_id_titles(inner):
            if min_rank <= rid <= max_rank:
                jobs.append((f"skill-rank-{rid}", "技能提升", rid, title))
    return jobs


def build_image_prompt(client: OpenAI, video_title: str, section_label: str) -> str:
    """用 qwen-plus 将栏目视频标题改写成适合文生图的描述（中英混合输出亦可）。"""
    system = (
        "你是视觉设计助理。用户会给出视频列表标题。请只输出一段中文文生图描述（80～200字），"
        "用于生成横版视频封面图：电影感、高细节、无文字无水印无Logo、无边框、"
        "适合深色UI缩略图，不要复述标签如【精选】。"
    )
    user = f"栏目：{section_label}\n视频标题：{video_title.strip()}"
    completion = client.chat.completions.create(
        model="qwen-plus",
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.7,
    )
    out = (completion.choices[0].message.content or "").strip()
    if not out:
        raise RuntimeError("qwen-plus 未返回有效文生图描述")
    return out


def create_text2image_task(
    api_key: str,
    prompt: str,
    model: str,
    size: str,
    watermark: bool,
) -> str:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "X-DashScope-Async": "enable",
    }
    body = {
        "model": model,
        "input": {"prompt": prompt},
        "parameters": {"size": size, "n": 1, "prompt_extend": True, "watermark": watermark},
    }
    r = requests.post(DASHSCOPE_T2I_URL, headers=headers, json=body, timeout=120)
    r.raise_for_status()
    data = r.json()
    if data.get("code"):
        raise RuntimeError(f"文生图创建任务失败: {data.get('code')} {data.get('message')}")
    task_id = (data.get("output") or {}).get("task_id")
    if not task_id:
        raise RuntimeError(f"响应缺少 task_id: {json.dumps(data, ensure_ascii=False)[:800]}")
    return task_id


def poll_task_result(api_key: str, task_id: str, timeout_s: int = 300, interval_s: float = 3.0) -> str:
    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"{DASHSCOPE_TASKS_URL}/{task_id}"
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = requests.get(url, headers=headers, timeout=60)
        r.raise_for_status()
        data = r.json()
        out = data.get("output") or {}
        status = out.get("task_status")
        if status == "SUCCEEDED":
            results = out.get("results") or []
            for item in results:
                u = item.get("url")
                if u:
                    return u
            raise RuntimeError(f"SUCCEEDED 但未找到 url: {json.dumps(out, ensure_ascii=False)[:800]}")
        if status == "FAILED":
            raise RuntimeError(
                f"任务失败: {out.get('code')} {out.get('message')} "
                f"{json.dumps(out, ensure_ascii=False)[:800]}"
            )
        time.sleep(interval_s)
    raise TimeoutError(f"等待任务 {task_id} 超时 ({timeout_s}s)")


def download_image(url: str) -> Image.Image:
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    from io import BytesIO

    return Image.open(BytesIO(r.content)).convert("RGB")


def paste_cover(
    base: Image.Image,
    cover: Image.Image,
    box: tuple[int, int, int, int],
) -> Image.Image:
    """box: (x1,y1,x2,y2) 包含右下像素；将 cover 缩放后贴入（居中裁剪为目标宽高比）。"""
    x1, y1, x2, y2 = box
    tw, th = x2 - x1, y2 - y1
    if tw <= 0 or th <= 0:
        raise ValueError("无效的粘贴区域")

    cw, ch = cover.size
    target_ratio = tw / th
    src_ratio = cw / ch
    if src_ratio > target_ratio:
        # 过宽，裁左右
        new_w = int(ch * target_ratio)
        left = (cw - new_w) // 2
        crop = cover.crop((left, 0, left + new_w, ch))
    else:
        new_h = int(cw / target_ratio)
        top = (ch - new_h) // 2
        crop = cover.crop((0, top, cw, top + new_h))
    resized = crop.resize((tw, th), Image.Resampling.LANCZOS)

    out = base.copy()
    out.paste(resized, (x1, y1))
    return out


def parse_region(s: str) -> tuple[int, int, int, int]:
    parts = [p.strip() for p in s.replace(" ", "").split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("区域格式应为 x1,y1,x2,y2")
    return tuple(int(p) for p in parts)  # type: ignore[return-value]


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    rank_js = root / "FrontEnd" / "src" / "api" / "rank.js"

    parser = argparse.ArgumentParser(description="频道榜视频封面：从 rank.js 读标题 → qwen-plus → 万相文生图")
    parser.add_argument(
        "--rank-js",
        type=Path,
        default=rank_js,
        help="解析 MOCK_MAIN_VIDEO / MOCK_SUB_VIDEO 列表",
    )
    parser.add_argument(
        "--min-rank",
        type=int,
        default=1,
        help="每个栏目从第几名开始生成（含边界，用于断点续跑）",
    )
    parser.add_argument(
        "--max-rank",
        type=int,
        default=5,
        help="每个栏目最多生成到第几名（与 VIDEO_RANK_SIZE 一致）",
    )
    parser.add_argument(
        "--only",
        choices=("creative", "skill", "both"),
        default="both",
        help="只生成哪一类封面",
    )
    parser.add_argument("--t2i-model", default=os.getenv("DASHSCOPE_T2I_MODEL", DEFAULT_T2I_MODEL))
    parser.add_argument(
        "--size",
        default="1280*1280",
        help="文生图 size，需符合模型文档像素与比例要求",
    )
    parser.add_argument("--watermark", action="store_true", help="为 true 时加「AI生成」水印")
    parser.add_argument(
        "--skip-llm",
        action="store_true",
        help="跳过 qwen-plus，直接用原标题作为文生图 prompt",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=root / "scripts" / "channel_cover_output",
        help="生成 png 与元数据输出目录",
    )
    parser.add_argument(
        "--public-dir",
        type=Path,
        default=root / "FrontEnd" / "public" / "channel-covers",
        help="若未加 --no-copy-public，则将 png 复制到此目录（供 Vite 静态引用）",
    )
    parser.add_argument(
        "--no-copy-public",
        action="store_true",
        help="不复制到 public/channel-covers",
    )
    parser.add_argument(
        "--composite",
        action="store_true",
        help="将生成图贴入截图（仅当 --max-rank 为 1 且仅一条任务时可用）",
    )
    parser.add_argument(
        "--base-image",
        type=Path,
        default=None,
        help="被合成的截图路径",
    )
    parser.add_argument(
        "--region",
        type=parse_region,
        default=None,
        help="粘贴区域 x1,y1,x2,y2；未指定且基图为 1024x728 时使用内置估算",
    )
    parser.add_argument("--dry-run", action="store_true", help="不调用 API，只列出将要生成的条目")
    args = parser.parse_args()

    api_key = os.getenv("DASHSCOPE_API_KEY", "").strip()
    needs_llm = not args.dry_run and not args.skip_llm
    needs_t2i = not args.dry_run
    if not api_key and (needs_llm or needs_t2i):
        print("请设置环境变量 DASHSCOPE_API_KEY", file=sys.stderr)
        return 1

    if args.min_rank < 1 or args.min_rank > args.max_rank:
        print("--min-rank 须 >=1 且 <= --max-rank", file=sys.stderr)
        return 1

    args.out_dir.mkdir(parents=True, exist_ok=True)
    if not args.no_copy_public:
        args.public_dir.mkdir(parents=True, exist_ok=True)

    if args.composite and not args.base_image:
        print("使用 --composite 时必须指定 --base-image", file=sys.stderr)
        return 1

    try:
        rank_jobs = load_rank_jobs(args.rank_js, args.min_rank, args.max_rank, args.only)
    except ValueError as e:
        print(e, file=sys.stderr)
        return 1

    if not rank_jobs:
        print("没有可生成的任务（检查 rank.js 与 --min-rank / --max-rank）", file=sys.stderr)
        return 1

    if args.composite and len(rank_jobs) != 1:
        print("--composite 仅支持本次只生成 1 张（调整 --min-rank 与 --max-rank）", file=sys.stderr)
        return 1

    client: OpenAI | None = None
    if needs_llm:
        client = OpenAI(
            api_key=api_key,
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        )

    meta: dict = {"jobs": []}
    for basename, section, rank_id, title in rank_jobs:
        if args.dry_run or args.skip_llm:
            image_prompt = title
        else:
            assert client is not None
            image_prompt = build_image_prompt(client, title, section)

        print(f"\n=== {section} #{rank_id} ===\n原标题: {title}\n文生图提示:\n{image_prompt}\n")
        rec: dict = {
            "basename": basename,
            "section": section,
            "rank_id": rank_id,
            "title": title,
            "image_prompt": image_prompt,
        }
        meta["jobs"].append(rec)

        if args.dry_run:
            continue

        task_id = create_text2image_task(
            api_key,
            image_prompt,
            model=args.t2i_model,
            size=args.size,
            watermark=args.watermark,
        )
        img_url = poll_task_result(api_key, task_id)
        rec["task_id"] = task_id
        rec["image_url"] = img_url
        img = download_image(img_url)
        out_png = args.out_dir / f"{basename}.png"
        img.save(out_png)
        print(f"已保存: {out_png}")
        if not args.no_copy_public:
            pub_path = args.public_dir / f"{basename}.png"
            shutil.copy2(out_png, pub_path)
            print(f"已复制: {pub_path}")

        if args.composite and args.base_image:
            base = Image.open(args.base_image).convert("RGB")
            region = args.region
            if region is None and base.size[0] == 1024 and base.size[1] == 728:
                region = (40, 120, 983, 519)
            if region is None:
                print("未指定 --region 且基图尺寸不是 1024x728，跳过合成", file=sys.stderr)
            else:
                composited = paste_cover(base, img, region)
                comp_path = args.out_dir / f"{basename}_composited.png"
                composited.save(comp_path)
                print(f"已合成: {comp_path}")

        time.sleep(1.0)

    meta_path = args.out_dir / "meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n元数据: {meta_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
