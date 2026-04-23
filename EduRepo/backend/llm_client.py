from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import httpx


DASHSCOPE_COMPAT_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"


_REQUIRED_SECTIONS = [
    "## 这件事在讲什么（一句话 + 3~5 句解释）",
    "## 为什么重要（和你有什么关系）（讲影响/应用/避免误解）",
    "## 用一个例子讲明白（生活化类比/场景）",
    "## 你可以怎么开始（3条可执行建议）（必须 3 条 - 列表）",
]


def _find_env_file() -> Optional[Path]:
    """
    Try to locate existing env files in this workspace.
    Priority: local overrides -> backend service envs -> repo envs.
    """
    here = Path(__file__).resolve()
    candidates: list[Path] = [here.parent / ".env", here.parent / ".env.local"]

    base_dirs: list[Path] = []
    for base in [
        here.parent.parent.parent,  # e.g. PP
        here.parent.parent.parent.parent,  # workspace root
    ]:
        if base and base not in base_dirs:
            base_dirs.append(base)

    for base in base_dirs:
        candidates.extend(
            [
                # monorepo layout
                base / "Backend" / "Crawl" / ".env",
                base / "Backend" / "WorkShop" / ".env",
                base / "Backend" / ".env",
                base / "Backend" / ".env.local",
                # legacy layout in this workspace
                base / "Backend-main" / "Crawl" / ".env",
                base / "Backend-main" / "WorkShop" / ".env",
                base / "Backend-main" / "Agent-Do" / ".env",
                # nested monorepo (when running from root EduRepo/)
                base / "PP" / "Backend" / "Crawl" / ".env",
                base / "PP" / "Backend" / "WorkShop" / ".env",
                base / "PP" / "Backend" / ".env",
                base / "PP" / "Backend" / ".env.local",
                base / ".env",
                base / ".env.local",
            ]
        )

    # extra fallback near this module
    candidates.extend(
        [
            here.parent.parent / ".env",
            here.parent.parent / ".env.local",
        ]
    )
    for path in candidates:
        if path.exists():
            return path
    return None


def _load_env_file() -> None:
    path = _find_env_file()
    if not path:
        return
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    except Exception:
        pass


def _get_llm_config() -> Dict[str, str]:
    _load_env_file()

    api_key = (
        os.getenv("EDUREPO_LLM_API_KEY")
        or os.getenv("OPENAI_API_KEY")
        or os.getenv("QWEN_API_KEY")
        or os.getenv("DASHSCOPE_API_KEY")
        or ""
    ).strip()

    base_url = (
        os.getenv("EDUREPO_LLM_BASE_URL")
        or os.getenv("OPENAI_BASE_URL")
        or os.getenv("QWEN_BASE_URL")
        or os.getenv("DASHSCOPE_BASE_URL")
        or ""
    ).strip()

    dashscope_key = bool(os.getenv("DASHSCOPE_API_KEY"))
    if dashscope_key and not base_url:
        base_url = DASHSCOPE_COMPAT_BASE_URL
    if api_key and not base_url:
        base_url = "https://api.openai.com/v1"

    model = (
        os.getenv("EDUREPO_LLM_MODEL")
        or os.getenv("EDUREPO_LLM_DEFAULT_MODEL")
        or os.getenv("DASHSCOPE_PROMPT_MODEL")
        or os.getenv("OPENAI_MODEL")
        or os.getenv("QWEN_MODEL")
        or os.getenv("DEFAULT_MODEL")
        or ""
    ).strip()

    # Avoid provider-prefixed DEFAULT_MODEL values from OpenMAIC (e.g. anthropic:xxx)
    if ":" in model and not model.lower().startswith(("gpt", "qwen", "deepseek", "glm", "kimi", "moonshot")):
        model = ""
    if not model:
        model = "qwen-plus" if dashscope_key else "gpt-4o-mini"

    if not api_key:
        raise ValueError("缺少 LLM API Key，请检查 EDUREPO_LLM_API_KEY / DASHSCOPE_API_KEY / OPENAI_API_KEY")
    if not base_url:
        raise ValueError("缺少 LLM Base URL，请检查 EDUREPO_LLM_BASE_URL / DASHSCOPE_BASE_URL / OPENAI_BASE_URL")

    return {"api_key": api_key, "base_url": base_url.rstrip("/"), "model": model}


async def chat_completion(
    system: str, user: str, max_tokens: int = 1600, cfg: Optional[Dict[str, str]] = None
) -> str:
    cfg = cfg or _get_llm_config()
    url = f"{cfg['base_url']}/chat/completions"
    headers = {"Authorization": f"Bearer {cfg['api_key']}", "Content-Type": "application/json"}
    payload = {
        "model": cfg["model"],
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "temperature": 0.4,
        "max_tokens": int(max_tokens),
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError("LLM 返回内容异常")
        choices = data.get("choices") or []
        if not choices:
            raise ValueError("LLM 返回结果格式异常：缺少 choices")
        msg = (choices[0] or {}).get("message") or {}
        return str(msg.get("content") or "").strip()


def extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    s = str(text).strip()
    start = s.find("{")
    end = s.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    snippet = s[start : end + 1]
    try:
        return json.loads(snippet)
    except Exception:
        return None


def _is_mostly_chinese(text: str) -> bool:
    s = (text or "").strip()
    if not s:
        return False
    cjk = len(re.findall(r"[\u4e00-\u9fff]", s))
    return cjk >= max(12, int(len(s) * 0.25))


def _prompt_inputs(article: Dict[str, Any]) -> Tuple[str, str, str]:
    title = str(article.get("title") or "").strip()
    summary = str(article.get("summary") or "").strip()
    content = str(article.get("content") or "").strip()
    if len(content) > 8000:
        content = content[:8000].rstrip() + "..."
    return title, summary, content


def _normalize_ps_markdown(ps_markdown: str, title: str, summary: str, content: str) -> str:
    s = str(ps_markdown or "").strip()
    # strip fenced code blocks (frontend has a minimal markdown renderer)
    s = re.sub(r"```[\s\S]*?```", "", s).strip()

    def _strip_title_echo(text: str) -> str:
        """
        Remove common title-echo patterns in generated markdown.
        We still keep the required '## ...' headings; only remove lines that
        repeat the source title or present it as a standalone heading.
        """
        t0 = str(title or "").strip()
        if not t0:
            return str(text or "").strip()
        lines = str(text or "").replace("\r\n", "\n").split("\n")
        out: list[str] = []
        for raw in lines:
            t = raw.strip()
            if not t:
                out.append(raw)
                continue
            if t == t0:
                continue
            if any(t.startswith(p) for p in ("标题：", "原文标题：", "文章标题：")) and (t0 in t):
                continue
            # Remove a top heading that mirrors the title (but keep required sections).
            if (t.startswith("#") and (t0 in t)) and (t not in _REQUIRED_SECTIONS):
                # if it's a hashtag line like "#AI #大模型 ..." it typically has multiple '#'
                if t.count("#") >= 2:
                    out.append(raw)
                else:
                    continue
            else:
                out.append(raw)
        return "\n".join(out).strip()

    def _strip_template_headings(text: str) -> str:
        """
        Remove the fixed template headings (and close variants) from the final display markdown.
        This keeps the content but avoids showing scaffolding like:
        '## 这件事在讲什么（一句话 + 3~5 句解释）'
        """
        banned_exact = set(_REQUIRED_SECTIONS)
        banned_re = re.compile(
            r"^##\s*(这件事在讲什么|为什么重要|用一个例子讲明白|你可以怎么开始)\b"
        )
        out: list[str] = []
        for raw in str(text or "").replace("\r\n", "\n").split("\n"):
            t = raw.strip()
            if t in banned_exact:
                continue
            if banned_re.match(t or ""):
                continue
            out.append(raw)
        return "\n".join(out).strip()

    def _indent_first_paragraph(text: str) -> str:
        """
        Prefer ideographic indentation for the first paragraph in a section:
        '　　' (2 full-width spaces). Avoid leading ASCII spaces to prevent code blocks.
        """
        lines = str(text or "").replace("\r\n", "\n").split("\n")
        for i, raw in enumerate(lines):
            t = raw.strip()
            if not t:
                continue
            # do not indent markdown headings
            if t.startswith("#"):
                continue
            if t.startswith(("　　", "- ", "* ", "> ")):
                return "\n".join(lines).strip("\n")
            lines[i] = "　　" + t
            return "\n".join(lines).strip("\n")
        return "\n".join(lines).strip("\n")

    def _hashtag_line(text: str) -> str:
        s2 = str(text or "").replace("\r\n", "\n")
        for ln in reversed(s2.split("\n")):
            t = ln.strip()
            if not t:
                continue
            if t.startswith("##"):
                continue
            if t.startswith("#") and t.count("#") >= 2 and " #" in t:
                return t
        return ""

    def _normalize_action_block(full_text: str) -> str:
        """
        Ensure the last action list is exactly 3 '-' bullets; preserve a trailing hashtag line.
        If no bullets exist, returns an empty string.
        """
        s2 = str(full_text or "").replace("\r\n", "\n")
        lines = s2.split("\n")
        last_bullet = -1
        for i, ln in enumerate(lines):
            if ln.strip().startswith(("- ", "* ", "• ")):
                last_bullet = i
        if last_bullet < 0:
            return ""

        # find contiguous bullet block
        start = last_bullet
        while start - 1 >= 0 and lines[start - 1].strip().startswith(("- ", "* ", "• ")):
            start -= 1
        end = last_bullet + 1
        while end < len(lines) and lines[end].strip().startswith(("- ", "* ", "• ")):
            end += 1

        block = "\n".join(lines[start:end]).strip()
        ht = _hashtag_line(s2)
        if ht:
            block = (block + "\n\n" + ht).strip()
        fixed = _ensure_three_bullets(block)
        return fixed

    def _bullets_to_paragraph(text: str) -> str:
        """
        If a section is mostly bullet points, turn it into a short paragraph.
        Best-effort only.
        """
        s2 = str(text or "").replace("\r\n", "\n").strip("\n")
        raw_lines = [ln.strip() for ln in s2.split("\n") if ln.strip()]
        if not raw_lines:
            return ""
        items: list[str] = []
        for ln in raw_lines:
            m = re.match(r"^(?:[-*•]\s+|\d+[.)]\s+)(.+)$", ln)
            if not m:
                return _indent_first_paragraph(s2)
            it = m.group(1).strip()
            if it:
                items.append(it)
        if not items:
            return _indent_first_paragraph(s2)
        joined = "；".join(items[:5]).rstrip("。；") + "。"
        return _indent_first_paragraph(joined)

    def _compose_article_from_sections(parts: Dict[str, str], original_full: str) -> str:
        what = _bullets_to_paragraph(parts.get(_REQUIRED_SECTIONS[0]) or "")
        why = _bullets_to_paragraph(parts.get(_REQUIRED_SECTIONS[1]) or "")
        ex = parts.get(_REQUIRED_SECTIONS[2]) or ""
        ex = _indent_first_paragraph(_strip_title_echo(ex))

        action_fixed = _ensure_three_bullets(parts.get(_REQUIRED_SECTIONS[3]) or "")
        ht = _hashtag_line(original_full) or _hashtag_line(action_fixed)
        if ht and ht in action_fixed:
            # keep only in tail
            action_fixed = "\n".join([ln for ln in action_fixed.splitlines() if ln.strip() != ht]).strip()

        out: list[str] = []
        if what:
            out.append(what)
        if why:
            out.append(why)
        if ex:
            out.append("### 一个例子")
            out.append(ex)
        out.append("### 你可以怎么开始")
        out.append(action_fixed.strip())
        if ht:
            out.append(ht)
        return "\n\n".join([x for x in out if x and str(x).strip()]).strip("\n")

    def _coerce_required_headings(text: str) -> str:
        lines = str(text or "").replace("\r\n", "\n").split("\n")
        out: list[str] = []
        for raw in lines:
            t = raw.strip()
            if not t.startswith("##"):
                out.append(raw)
                continue

            # normalize possible variants into required headings (line-based, conservative)
            if ("讲什么" in t) and ("例子" not in t) and ("开始" not in t):
                out.append(_REQUIRED_SECTIONS[0])
                continue
            if ("为什么" in t) or ("重要" in t and "例子" not in t):
                out.append(_REQUIRED_SECTIONS[1])
                continue
            if ("例子" in t) or ("举例" in t) or ("类比" in t):
                out.append(_REQUIRED_SECTIONS[2])
                continue
            if ("怎么开始" in t) or ("开始" in t and ("建议" in t or "行动" in t or "做" in t)):
                out.append(_REQUIRED_SECTIONS[3])
                continue

            out.append(raw)
        return "\n".join(out).strip()

    def _split_required_sections(text: str) -> Optional[Dict[str, str]]:
        lines = str(text or "").replace("\r\n", "\n").split("\n")
        idxs: list[int] = []
        for h in _REQUIRED_SECTIONS:
            try:
                idxs.append(next(i for i, line in enumerate(lines) if line.strip() == h))
            except StopIteration:
                return None
        if idxs != sorted(idxs):
            return None

        parts: dict[str, str] = {}
        for i, h in enumerate(_REQUIRED_SECTIONS):
            start = idxs[i] + 1
            end = idxs[i + 1] if i + 1 < len(idxs) else len(lines)
            parts[h] = "\n".join(lines[start:end]).strip()
        return parts

    def _ensure_three_bullets(text: str) -> str:
        raw_lines = [ln.rstrip() for ln in str(text or "").replace("\r\n", "\n").split("\n")]
        lines = [ln for ln in raw_lines if ln.strip()]
        items: list[str] = []
        hashtag_tail = ""

        # Preserve a trailing hashtag line like: "#大模型 #AI工具 #RAG ..."
        for ln in reversed(lines):
            t = ln.strip()
            if not t:
                continue
            if t.startswith("##"):
                continue
            if t.startswith("#") and t.count("#") >= 2:
                hashtag_tail = t
                break

        for ln in lines:
            t = ln.strip()
            # accept -, *, •, 1. 2) styles
            m = re.match(r"^(?:[-*•]\s+|\d+[.)]\s+)(.+)$", t)
            if not m:
                continue
            it = m.group(1).strip()
            if not it:
                continue
            if it in items:
                continue
            items.append(it)
            if len(items) >= 3:
                break

        while len(items) < 3:
            for fallback in [
                "先用一句话复述这个概念（讲给朋友听）",
                "挑一个你关心的场景做 10 分钟小实验",
                "写下 3 个你还不懂的问题，下一步去查",
            ]:
                if fallback not in items:
                    items.append(fallback)
                if len(items) >= 3:
                    break

        items = items[:3]
        out = "\n".join([f"- {it}" for it in items]).strip()
        if hashtag_tail:
            out = f"{out}\n\n{hashtag_tail}".strip()
        return out

    if not s:
        # No LLM output: keep stable structure but avoid echoing the raw title.
        return _compose_article_from_sections(
            {
                _REQUIRED_SECTIONS[0]: (summary or "").strip()
                or "这篇内容在讲一个核心概念：它是什么、为什么重要，以及你可以怎么开始上手。",
                _REQUIRED_SECTIONS[1]: "它会影响你理解 AI、选择工具、甚至学习与工作的方式。",
                _REQUIRED_SECTIONS[2]: (content[:420].strip() if content else "原文内容较少，建议稍后重试。"),
                _REQUIRED_SECTIONS[3]: "\n".join(
                    [
                        "- 先把关键词查一遍含义（别死记）",
                        "- 找一个你关心的场景做小实验",
                        "- 记录你学到的 3 点，反复复习",
                    ]
                ),
            },
            "",
        )

    s = _strip_title_echo(s)
    # If the model output still used the old template headings, parse it and re-compose.
    parts = _split_required_sections(_coerce_required_headings(s))
    if parts:
        # Old-style template output -> compose into a cohesive article without the template headings.
        fixed = _ensure_three_bullets(parts.get(_REQUIRED_SECTIONS[-1]) or "")
        # If the model already provided hashtags, keep them in tail.
        ht = _hashtag_line(s) or _hashtag_line(fixed)
        if ht and ht in fixed:
            fixed = "\n".join([ln for ln in fixed.splitlines() if ln.strip() != ht]).strip()
        parts[_REQUIRED_SECTIONS[-1]] = fixed
        return _compose_article_from_sections(parts, s)

    # New-style article output (no template headings expected).
    s = _strip_template_headings(s)
    # - ensure first paragraph has ideographic indentation
    s = _indent_first_paragraph(s)
    # - normalize the last action bullets if present
    fixed_actions = _normalize_action_block(s)
    if fixed_actions:
        # remove existing bullet block and append the fixed one at the end under a simple heading
        ht = _hashtag_line(s) or _hashtag_line(fixed_actions)
        # strip all bullet lines at the end (best-effort)
        lines = [ln.rstrip() for ln in s.replace("\r\n", "\n").split("\n")]
        # remove trailing hashtag line temporarily
        while lines and not lines[-1].strip():
            lines.pop()
        if lines and ht and lines[-1].strip() == ht:
            lines.pop()
            while lines and not lines[-1].strip():
                lines.pop()
        # remove trailing bullet block
        while lines and lines[-1].strip().startswith(("- ", "* ", "• ")):
            lines.pop()
        # also remove a trailing template-like heading if present
        while lines and re.match(r"^##\s*你可以怎么开始", lines[-1].strip() or ""):
            lines.pop()
        while lines and re.match(r"^###\s*你可以怎么开始", lines[-1].strip() or ""):
            lines.pop()
        s2 = "\n".join(lines).strip("\n")
        tail: list[str] = ["### 你可以怎么开始", fixed_actions.strip()]
        if ht and ht not in fixed_actions:
            tail.append(ht)
        return "\n\n".join([x for x in [s2] + tail if x and x.strip()]).strip("\n")

    # If no bullets found, append a default action block.
    default_actions = "\n".join(
        [
            "- 先用一句话复述你理解到的“关键变化”",
            "- 找一个你熟悉的场景套进去做 10 分钟小实验",
            "- 写下 3 个你还不懂的问题，下一步去查",
        ]
    )
    return "\n\n".join([x for x in [s.strip("\n"), "### 你可以怎么开始", default_actions] if x and x.strip()]).strip("\n")


async def popularize_article(article: Dict[str, Any]) -> Dict[str, Any]:
    """
    Translate (if needed) and rewrite into Chinese popular science with Markdown structure.
    """
    title, summary, content = _prompt_inputs(article)
    src_is_cn = _is_mostly_chinese(title + "\n" + summary + "\n" + content)

    system = (
        "你是一位中文科普写作专家，擅长把技术文章改写成人人都能看懂的内容。"
        "写作要求：准确、不夸张、不卖课、不引流；少术语，必要术语先解释；"
        "用生活化比喻辅助理解；段落短；重点用要点列表呈现；"
        "结尾给出“我该怎么用/怎么开始”的建议。"
    )

    user = (
        "请根据下面原文内容，输出一个 JSON 对象（只输出 JSON，不要输出其它文本）。\n"
        "如果原文不是中文，请先翻译并改写成通俗中文；如果原文已经是中文，也要进一步“科普化改写”。\n\n"
        "重要写作风格（用于解决“洗稿/照搬标题”的问题）：\n"
        "- 不要照抄原文句子；禁止输出连续 20 字以上的原文原句（除非必要且加引号，但尽量避免）。\n"
        "- 不要在 ps_markdown 里出现“标题：…/原文标题：…”或把原文标题整句复述出来；ps_title 也不要与原文标题完全相同。\n"
        "- ps_markdown 的正文要像“小红书科普笔记”：段落短、逻辑清楚、比喻自然、读起来像一段一段的说明文字。\n"
        "- 正文前 3 个小节（讲什么/为什么重要/例子）优先用 2~4 段短文字表达，**不要用 '-' 列表**（除非段内插入 1 条非常短的补充要点）。\n"
        "- 每个小节的第一段建议用两个全角空格“　　”做首行缩进（不要用 4 个空格，避免变成代码块）。\n\n"
        "JSON 字段要求：\n"
        "- ps_title：15-24字中文标题，像小红书一样抓眼但不标题党（不要直接复用原文标题）\n"
        "- ps_summary：80-120字中文摘要，面向零基础读者\n"
        "- ps_markdown：一篇连贯的 Markdown 科普短文（不要代码块；不要用“标题：”行；不要把原文标题整句复述出来）。\n"
        "  要求：\n"
        "  1) 开头用 1~2 段把“这件事在讲什么”讲清楚（像讲给朋友听），首段首行缩进用“　　”。\n"
        "  2) 接着用 1~2 段解释“为什么重要/和你有什么关系”。\n"
        "  3) 再用 1~2 段写一个生活化场景例子（可以用一句“想象…”开头）。\n"
        "  4) 最后用一个小标题 `### 你可以怎么开始`，并严格输出 3 条以 `- ` 开头的可执行建议（只能 3 条）。\n"
        "  5) 最后一行输出 6~12 个话题标签（用空格分隔），格式类似：#大模型 #人工智能 #AI工具 ……（尽量来自 keywords）。\n"
        "  6) 除了 `### 你可以怎么开始` 外，正文不要使用 `#` 或 `##` 形式的标题行。\n"
        "  禁止在 ps_markdown 中出现这些模板小标题行：\n"
        "  - `## 这件事在讲什么（一句话 + 3~5 句解释）`\n"
        "  - `## 为什么重要（和你有什么关系）（讲影响/应用/避免误解）`\n"
        "  - `## 用一个例子讲明白（生活化类比/场景）`\n"
        "  - `## 你可以怎么开始（3条可执行建议）（必须 3 条 - 列表）`\n"
        "- keywords：数组，3-8个关键词（中文为主，必要时可含英文缩写如 RAG/LoRA）\n"
        "- highlights：数组，1-3个用于封面高亮的短词（尽量从 keywords 里选）\n"
        "- glossary：数组，每个元素包含 term 和 explain（term 最好是英文缩写或关键术语）\n"
        "- lang：原文语言（如 en/zh/ja...）\n\n"
        f"原文标题：{title}\n"
        f"原文摘要：{summary}\n"
        f"原文正文：{content}\n"
        f"提示：原文语言大概率为 {'zh' if src_is_cn else '非中文'}。\n"
    )

    cfg = _get_llm_config()
    used_model = str(cfg.get("model") or "").strip()
    raw = await chat_completion(system, user, max_tokens=1700, cfg=cfg)
    obj = extract_json_object(raw)
    if not isinstance(obj, dict):
        # soft fallback
        return {
            "ps_title": title or "AI 科普笔记",
            "ps_summary": summary or (content[:120].strip() if content else ""),
            "ps_markdown": "\n\n".join(
                [
                    _REQUIRED_SECTIONS[0],
                    (summary or title or "").strip(),
                    _REQUIRED_SECTIONS[1],
                    "这会影响你理解 AI、选择工具、甚至工作方式。",
                    _REQUIRED_SECTIONS[2],
                    (content[:420].strip() if content else "（原文内容较少，建议稍后重试）"),
                    _REQUIRED_SECTIONS[3],
                    "- 先把关键词查一遍含义（别死记）",
                    "- 找一个你关心的场景做小实验",
                    "- 记录你学到的 3 点，反复复习",
                ]
            ),
            "keywords": [],
            "highlights": [],
            "glossary": [],
            "lang": "zh" if src_is_cn else "",
            "llm_model": used_model,
        }

    def _clean_list(xs, limit: int) -> list[str]:
        out: list[str] = []
        if not isinstance(xs, list):
            return out
        for x in xs[:limit]:
            s = str(x or "").strip()
            if not s or s in out:
                continue
            out.append(s[:32])
        return out

    keywords = _clean_list(obj.get("keywords"), 10)
    highlights = _clean_list(obj.get("highlights"), 3) or keywords[:2]

    clean_glossary: list[dict] = []
    for it in obj.get("glossary") or []:
        if not isinstance(it, dict):
            continue
        term = str(it.get("term") or "").strip()
        explain = str(it.get("explain") or "").strip()
        if not term or not explain:
            continue
        clean_glossary.append({"term": term[:40], "explain": explain[:140]})
        if len(clean_glossary) >= 10:
            break

    ps_title = str(obj.get("ps_title") or "").strip()[:40] or title
    ps_summary = str(obj.get("ps_summary") or "").strip()[:500] or summary
    ps_markdown = _normalize_ps_markdown(str(obj.get("ps_markdown") or ""), title, summary, content)[:9000]
    lang = str(obj.get("lang") or ("zh" if src_is_cn else "")).strip()[:8]

    # Avoid echoing the original title verbatim (common with news "洗稿" outputs).
    if title and ps_title and ps_title.strip() == title.strip():
        concept = (keywords[0] if keywords else "").strip()
        if concept and len(concept) <= 10:
            ps_title = f"3分钟搞懂：{concept} 到底是什么"
        else:
            ps_title = "3分钟搞懂：这条新闻到底在讲什么"

    return {
        "ps_title": ps_title,
        "ps_summary": ps_summary,
        "ps_markdown": ps_markdown,
        "keywords": keywords,
        "highlights": highlights,
        "glossary": clean_glossary,
        "lang": lang,
        "llm_model": used_model,
    }
