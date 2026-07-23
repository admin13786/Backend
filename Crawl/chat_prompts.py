import json
import re
from typing import Any, Dict, List, Sequence


def extract_json_object(text: str) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    if "```" in raw:
        for part in raw.split("```"):
            chunk = part.strip()
            if chunk.startswith("json"):
                chunk = chunk[4:].strip()
            if chunk.startswith("{") and chunk.endswith("}"):
                raw = chunk
                break
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        raw = raw[start : end + 1]
    try:
        value = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def normalize_message_text(value: str, limit: int = 600) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def build_intent_prompt(
    message: str,
    history: Sequence[Dict[str, Any]],
    available_sources: Sequence[str],
) -> str:
    history_lines: List[str] = []
    for item in list(history)[-6:]:
        role = str(item.get("role") or "").strip() or "user"
        content = normalize_message_text(item.get("content") or "", 200)
        if content:
            history_lines.append(f"{role}: {content}")
    history_text = "\n".join(history_lines) if history_lines else "(empty)"
    source_text = ", ".join(str(item or "").strip() for item in available_sources if str(item or "").strip())
    return f"""你是新闻问答机器人的意图路由器。
任务：只根据当前用户消息和最近对话，判断该如何检索新闻库并回答。

可用新闻源：{source_text or "(unknown)"}
可选意图：
- news_qa：围绕新闻内容提问、解释、追问
- latest_news：找最新/最近的新闻
- summary：总结某个主题或一批新闻
- compare：比较两条或多条新闻
- recommend：推荐值得看/关注的新闻
- follow_up：承接上一轮上下文的追问

要求：
1. 只输出 JSON，不要 Markdown，不要解释。
2. 如果用户明确问“最新、最近、今天”，优先 latest_news。
3. 如果用户提到某个来源、媒体、网站，请在 source_keys 里给出对应来源键。
4. 如果用户问题很泛，query 里写出最有检索价值的关键词。
5. days 用整数天数，默认可留空或给 7。

最近对话：
{history_text}

用户消息：
{normalize_message_text(message, 500)}

输出格式：
{{
  "intent": "news_qa",
  "query": "检索关键词",
  "source_keys": ["hacker_news"],
  "days": 7,
  "need_clarification": false,
  "clarification_question": "",
  "answer_style": "concise"
}}"""


def build_answer_prompt(
    message: str,
    intent: Dict[str, Any],
    citations: Sequence[Dict[str, Any]],
    history: Sequence[Dict[str, Any]],
) -> str:
    history_lines: List[str] = []
    for item in list(history)[-8:]:
        role = str(item.get("role") or "").strip() or "user"
        content = normalize_message_text(item.get("content") or "", 240)
        if content:
            history_lines.append(f"{role}: {content}")
    history_text = "\n".join(history_lines) if history_lines else "(empty)"

    citation_lines: List[str] = []
    for citation in citations:
        citation_lines.append(
            f"[{citation.get('index')}] {citation.get('title')} | {citation.get('source')} | "
            f"{citation.get('publishedAt')} | {normalize_message_text(citation.get('snippet') or '', 180)}"
        )
    citation_text = "\n".join(citation_lines) if citation_lines else "(no evidence)"
    intent_text = json.dumps(intent or {}, ensure_ascii=False)

    return f"""你是一个基于新闻库的中文问答助手。
回答要求：
1. 只基于给定证据回答，不要编造。
2. 如果证据不足，明确说“没有找到足够证据”，并说明还缺什么。
3. 尽量简洁，默认 3-6 句。
4. 每个关键结论后面用 [n] 标注引用，n 对应下方证据编号。
5. 如果用户是在追问，承接上文，不要重复无关背景。
6. 输出中文，除非用户明显要求英文。

最近对话：
{history_text}

用户消息：
{normalize_message_text(message, 500)}

意图路由结果：
{intent_text}

证据：
{citation_text}

请直接输出最终回答正文。"""
