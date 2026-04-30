# Backend Deploy

这个 `deploy` 目录是云端后端部署入口。前端建议在本地或单独静态托管运行，不需要放进这套 Docker Compose。

## 目录结构

上传到云端后，后端仓库建议保持如下结构：

```text
Backend/
  Agent-Do/
  Crawl/
  EduRepo/
  OpenMAIC/
  WorkShop/
  deploy/
```

## 服务

默认启动：

- `crawl-api`: 资讯接口，默认端口 `8000`
- `crawl-monitor`: 抓取监控，默认端口 `6670`
- `agent-do`: 代码生成和预览会话服务，默认端口 `18000`
- `workshop`: 创意工坊 API，默认端口 `5000`
- `openmaic`: 内嵌 OpenMAIC，默认端口 `3000`

按 profile 可选启动：

- `crawl-worker`: `--profile crawler`
- `edurepo-backend`: `--profile edurepo`，默认端口 `9010`

## 第一次部署

进入仓库根目录：

```bash
cd ~/Backend
```

复制环境变量模板：

```bash
cp deploy/.env.example deploy/.env
```

编辑真实配置：

```bash
vi deploy/.env
```

至少需要确认：

```env
DASHSCOPE_API_KEY=你的真实值
ALIYUN_ANTHROPIC_API_KEY=你的真实值
AGENT_DATA_HOST_ROOT=/root/Backend/Agent-Do/data
DEFAULT_RUNTIME_PROFILE=aliyun
OPENMAIC_PORT=3000
```

如果 Workshop 需要生成公网 OSS URL，再配置：

```env
OSS_ACCESS_KEY_ID=
OSS_ACCESS_KEY_SECRET=
OSS_BUCKET_NAME=
OSS_ENDPOINT=
OSS_DOMAIN=
```

## 启动

```bash
chmod +x deploy/up.sh deploy/down.sh deploy/cloud-check.sh
./deploy/cloud-check.sh
./deploy/up.sh
```

带抓取 worker：

```bash
./deploy/up.sh --profile crawler
```

带 EduRepo：

```bash
./deploy/up.sh --profile edurepo
```

全部启动：

```bash
./deploy/up.sh --profile crawler --profile edurepo
```

## 查看状态

```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env ps
docker compose -f deploy/docker-compose.yml --env-file deploy/.env logs -f --tail=200
```

## 停止

```bash
./deploy/down.sh
```

## 云端构建注意事项

如果阿里云机器拉 Docker Hub 镜像不稳定，可以在 `deploy/.env` 里替换这些基础镜像为你自己的镜像仓库地址：

```env
CRAWL_PYTHON_IMAGE=python:3.10-slim
AGENT_DO_PYTHON_IMAGE=python:3.11-slim
AGENT_DO_DOCKER_CLI_IMAGE=docker:27-cli
WORKSHOP_PYTHON_IMAGE=python:3.11-slim
CLAUDE_NODE_IMAGE=node:20-slim
OPENMAIC_NODE_IMAGE=node:22-alpine3.19
EDUREPO_PYTHON_IMAGE=python:3.10-slim
APP_RUNTIME_IMAGE=node:20-alpine
```

包源也已参数化，默认使用国内源：

```env
CRAWL_PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
AGENT_DO_PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
WORKSHOP_PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
OPENMAIC_NPM_REGISTRY=https://registry.npmmirror.com
CLAUDE_NPM_REGISTRY=https://registry.npmmirror.com
```

## Workshop 运行策略

- 创建 Agent-Do session 不会立即创建 Claude 容器。
- Workshop 生成时才临时启动 Claude 容器。
- 生成结束后默认调用 `AGENT_DO_RELEASE_CLAUDE_AFTER_REQUEST=true` 释放 Claude 容器。
- 静态 `index.html` 预览不需要长期运行容器。
- 生成出来的 Node 项目预览才会启动 app runtime，按 `APP_RUNTIME_IDLE_TIMEOUT_SECONDS` 自动回收。
