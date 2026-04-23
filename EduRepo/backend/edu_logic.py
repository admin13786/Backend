from __future__ import annotations

import hashlib
import random
import re
from typing import Any, Dict, List, Tuple


CONCEPTS: list[tuple[str, list[str]]] = [
    ("RAG", [r"\bRAG\b", "检索增强", "Retrieval-Augmented"]),
    ("LoRA", [r"\bLoRA\b", "低秩适配", "Low-Rank Adaptation"]),
    ("Agent", [r"\bAgent\b", "智能体", "代理", "agentic"]),
    ("Transformer", [r"\bTransformer\b", "注意力机制", "Self-Attention"]),
    ("Embedding", [r"\bEmbedding\b", "向量", "向量化", "表征学习"]),
    ("微调", ["微调", "SFT", "指令微调", "finetune", "fine-tuning"]),
    ("对齐", ["对齐", "RLHF", "DPO", "偏好优化", "alignment"]),
    ("蒸馏", ["蒸馏", "distill", "distillation"]),
    ("推理", ["推理", "reasoning", "CoT", "链式思维"]),
    ("多模态", ["多模态", "multimodal", "VLM", "图文", "语音"]),
    ("MoE", [r"\bMoE\b", "混合专家", "Mixture of Experts"]),
    ("Token", [r"\btoken\b", "Token", "上下文窗口", "context window", "KV Cache"]),
]

LEARNING_HINTS: list[str] = [
    "是什么",
    "什么是",
    "入门",
    "科普",
    "一文读懂",
    "快速上手",
    "新手",
    "基础",
    "原理",
    "教程",
    "指南",
    "避坑",
    "图解",
    "总结",
]

LOW_QUALITY_HINTS: list[str] = [
    "招商",
    "加盟",
    "返利",
    "免费领取",
    "加微信",
    "私信",
    "课程报名",
    "名额有限",
]


def _stable_seed(*parts: str) -> int:
    s = "|".join((p or "").strip() for p in parts if p is not None)
    digest = hashlib.sha1(s.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def _match_any(patterns: List[str], text: str) -> bool:
    for pat in patterns:
        if not pat:
            continue
        if pat.startswith(r"\b") or any(ch in pat for ch in "[](){}.*+?|\\"):
            if re.search(pat, text, flags=re.I):
                return True
        else:
            if pat.lower() in text:
                return True
    return False


def extract_concepts(text: str, top_k: int = 4) -> List[str]:
    blob = (text or "").lower()
    found: list[str] = []
    for name, pats in CONCEPTS:
        if _match_any(pats, blob):
            found.append(name)
    return found[:top_k]


def edu_score(text: str) -> float:
    blob = (text or "").lower()
    if _match_any(LOW_QUALITY_HINTS, blob):
        return 0.0
    concept_hits = 0
    for _, pats in CONCEPTS:
        if _match_any(pats, blob):
            concept_hits += 1
    learn_hits = sum(1 for h in LEARNING_HINTS if h.lower() in blob)
    return concept_hits * 1.3 + min(learn_hits, 6) * 0.45


def pick_template_id(key: str) -> str:
    return f"t{(_stable_seed(key) % 4) + 1}"


def generate_hook_title(original_title: str, concepts: List[str], stable_key: str) -> Tuple[str, List[str]]:
    base = (original_title or "").strip()
    concept = concepts[0] if concepts else "大模型"
    rnd = random.Random(_stable_seed(stable_key, base, concept))
    templates = [
        "3分钟搞懂：{concept}到底是什么？",
        "新手必看：{concept}的{n}个关键点",
        "别再被{concept}绕晕：一句话讲透",
        "从0到1：{concept}入门路线图",
        "{concept}到底难在哪？用一个例子讲清楚",
    ]
    n = rnd.choice([3, 5, 7])
    hook = rnd.choice(templates).format(concept=concept, n=n)
    highlights: list[str] = [concept]
    for c in concepts[1:]:
        if c not in highlights:
            highlights.append(c)
    return hook, highlights[:3]

