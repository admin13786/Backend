import os
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv


_LOADED = False
_DASHSCOPE_KEY_ENV_NAMES = (
    "DASHSCOPE_API_KEY",
    "QWEN_API_KEY",
    "ALIYUN_ANTHROPIC_API_KEY",
)


def load_crawl_env() -> None:
    """
    统一加载 Crawl 服务所需环境变量。

    优先级：
    1. Backend/Crawl/.env        服务私有配置
    2. Backend/WorkShop/.env     后端共享配置
    3. 进程已有环境变量           优先级最高（load_dotenv override=False）
    """
    global _LOADED
    if _LOADED:
        return

    current_dir = Path(__file__).resolve().parent
    repo_root = current_dir.parent.parent
    candidates = [
        current_dir / ".llm.env",
        current_dir / ".env",
        current_dir.parent / "WorkShop" / ".env",
        repo_root / ".llm.env",
        repo_root / ".env",
    ]

    for env_path in candidates:
        if env_path.is_file():
            load_dotenv(env_path, override=False)

    _LOADED = True


def get_first_env(names: Iterable[str], default: str = "") -> str:
    for name in names:
        value = str(os.getenv(name, "") or "").strip()
        if value:
            return value
    return str(default or "").strip()


def get_dashscope_api_key() -> str:
    load_crawl_env()
    return get_first_env(_DASHSCOPE_KEY_ENV_NAMES, "")
