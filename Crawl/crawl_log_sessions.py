"""Worker 启动时将非空日志移入 archive/，便于按「第 N 次运行」查看历史。"""

import os
import shutil

from tz_display import batch_ts_suffix


def rotate_crawl_log_if_nonempty(log_path: str) -> str | None:
    """
    若 log_path 存在且非空，则移动到 logs/archive/crawl_log_<timestamp>.txt。
    返回归档后的绝对路径；若未归档则返回 None。
    """
    if not log_path or not os.path.exists(log_path) or os.path.getsize(log_path) == 0:
        return None
    root = os.path.dirname(os.path.abspath(log_path))
    archive = os.path.join(root, "archive")
    os.makedirs(archive, exist_ok=True)
    ts = batch_ts_suffix()
    base = os.path.basename(log_path)
    name, ext = os.path.splitext(base)
    dest = os.path.join(archive, f"{name}_{ts}{ext or '.txt'}")
    shutil.move(log_path, dest)
    return dest
