# 教育仓库（EduRepo）Demo - Backend

EduRepo 是一个“内容加工仓库”demo：**只通过现有 Crawl 服务的 API 取数**，用大模型把原文改写成“零基础也能看懂”的中文科普笔记（含吸睛标题、关键词、术语表），并存入本地 SQLite。前端只读 EduRepo 的数据库，因此浏览很快。

## 运行

确保 Crawl 服务可访问（默认 `http://localhost:8000`，也可以是云端地址）。

```powershell
cd D:\AI_coding\EduRepo\backend
python -m uvicorn app:app --host 0.0.0.0 --port 9010 --reload
```

## Docker

如果通过上层 `Backend/EduRepo/docker-compose.yml` 启动，这个服务会默认监听：

- `0.0.0.0:9010`

并使用以下默认值：

- `CRAWL_API_BASE=http://host.docker.internal:8000`
- `EDU_REPO_DATA_DIR=/app/data`

## 环境变量

- `CRAWL_API_BASE`：Crawl 服务地址，默认 `http://localhost:8000`
- `EDU_REPO_DATA_DIR`：数据目录（默认 `backend/data`），用于存 DB/封面缓存
- `EDU_FONT_PATH`：自定义字体路径（ttf/ttc），用于更好的中文粗体效果

LLM 配置（可选）：

- `EDUREPO_LLM_API_KEY`（或 `DASHSCOPE_API_KEY` / `OPENAI_API_KEY`）
- `EDUREPO_LLM_BASE_URL`（或 `OPENAI_BASE_URL` / `DASHSCOPE_BASE_URL`）
- `EDUREPO_LLM_MODEL`（不填会自动选择；若检测到 DashScope key 会默认 `qwen-plus`）

注：为方便复用现有配置，后端会尝试在 `PP/Backend/Crawl/.env`、`PP/Backend/WorkShop/.env`、`Backend-main/Crawl/.env` 等位置自动读取 key。

## 核心流程（DB-first）

1) `POST /api/edu/sync`：从 Crawl 拉取原文入库（只写 raw，不跑 LLM）
2) `POST /api/edu/process`：消费 pending/error 记录，调用 LLM 生成 `ps_*` 字段并写回 DB
3) `GET /api/edu/feed`：只读 DB（只返回已处理完成的记录），用于前端卡片流

## API

- `GET /health`
- `GET /api/edu/feed?limit=40&board=all&q=&minScore=1.4`
- `GET /api/edu/items/{newsId}`：返回 `psMarkdown`、`keywords`、`glossary`、`status`
- `POST /api/edu/sync?limit=80&board=all&q=&minScore=1.2`
- `POST /api/edu/process?limit=20&board=all`
- `POST /api/edu/backfill?limit=20&board=all&q=&minScore=1.2`（便捷接口：sync + process）
- `GET /api/edu/cover.png?templateId=t1&title=...&highlights=a,b&size=3x4`
