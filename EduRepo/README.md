# 教育仓库（EduRepo）小 Demo

目标：从 **现有 Crawl 服务**拉取数据（不改 Crawl），筛出“AI 新概念学习/科普”条目 → 生成吸睛标题 → 生成小红书风格封面图 → 前端瀑布流展示。

## 逻辑概览

1. EduRepo frontend 以**和现有 FrontEnd 相同的方式**调用 Crawl API：
   - `GET {Crawl}/api/ranks/main/weibo`、`GET {Crawl}/api/ranks/sub/weibo` 获取榜单条目（含 `newsId`）
   - `GET {Crawl}/api/news/{newsId}` 拉取详情（title/summary/content/source/url/cover_url…）
2. EduRepo frontend 在浏览器端做 **规则/关键词**筛选 + 打分（demo 版，不走 LLM）
3. 生成 `hookTitle`（吸睛标题，规则模板 + 稳定随机）
4. 用 Canvas 渲染 4 套模板封面（t1~t4），直接生成 `data:image/png;base64,...` 用于卡片展示
5. 瀑布流卡片展示；点卡片拉详情弹窗

## 运行方式（本机）

### 0) 启动 Crawl（现有项目）

在 `Backend-main/Crawl` 启动：

```bash
python -m uvicorn run_local:app --host 0.0.0.0 --port 8000
```

验证：
- `http://localhost:8000/health`
- `http://localhost:8000/api/ranks/main/weibo`

### 1) 启动 EduRepo frontend（Vite）

```bash
cd Backend/EduRepo/frontend
npm i
npm run dev
```

打开：
- `http://localhost:5188`

开发环境默认将 `/api/*` 代理到 `http://localhost:8000`（可用 `VITE_CRAWL_PROXY_TARGET` 覆盖，见 `Backend/EduRepo/frontend/.env.example:1`）。

## Demo 的可改点（下一步）

- 目前筛选/标题生成是规则版：下一步可接你现有的 LLM（DashScope/OpenAI 兼容）生成多候选标题并打分选优。
- 封面模板目前 4 套：可以按你想要的小红书风继续加（t5/t6…），前端无需改接口。

## Docker 启动

如果你希望把 EduRepo 本身也跑成容器，可直接在 `Backend/EduRepo/` 目录执行：

```bash
docker compose up -d --build
```

默认端口：

- `http://localhost:9010`：EduRepo Backend
- `http://localhost:5188/edurepo/`：EduRepo Frontend

说明：

- 容器里的 EduRepo Backend 默认访问 `http://host.docker.internal:8000` 作为 Crawl API。
- 因此请先确保宿主机上的 Crawl 已经启动，或者你显式设置 `CRAWL_API_BASE`。
- 如果你要接入现有主站左侧栏，主站保持 `VITE_EDUREPO_APP_URL=/edurepo/` 即可复用这个前端容器。

### 换源

如果 Docker 构建较慢，可以在启动前切换这些环境变量：

```bash
export EDUREPO_PYTHON_IMAGE=docker.m.daocloud.io/library/python:3.12-slim
export EDUREPO_NODE_IMAGE=docker.m.daocloud.io/library/node:20-alpine
export EDUREPO_NGINX_IMAGE=docker.m.daocloud.io/library/nginx:alpine
export EDUREPO_PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple
export EDUREPO_NPM_REGISTRY=https://registry.npmmirror.com

docker compose up -d --build
```

如果你本机 Docker daemon 已经配置了 `registry-mirrors`，这几个变量通常再配上面这些值就够用了。

### 为什么看起来会“卡死”

如果日志停在 `apt-get install fonts-noto-cjk`，通常不是前端卡住，而是后端镜像在安装中文字体包。这个包体积较大，下载和解包阶段输出很少，看起来像挂住。

现在默认已经关闭这个步骤。如果你后续确实需要更好的中文封面字体，再显式开启：

```bash
export EDUREPO_INSTALL_CJK_FONT=true
docker compose up -d --build
```
