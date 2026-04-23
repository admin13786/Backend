# Backend Deploy

这套 `deploy` 是给“只有后端仓库”的云端部署入口。

上传这个仓库后，服务端目录结构应当类似：

```text
Backend/
  Agent-Do/
  Crawl/
  EduRepo/
  OpenMAIC/
  WorkShop/
  deploy/
```

## 包含什么

- `docker-compose.yml`
  后端总 compose，默认起：
  - `crawl-api`
  - `crawl-monitor`
  - `agent-do`
  - `workshop`
- `.env.example`
  总环境变量模板
- `up.sh`
  一键启动脚本，会先构建 `claude-runtime:latest`，再拉起整套后端
- `down.sh`
  停止整套后端

## 第一次部署

进入仓库根目录：

```bash
cd ~/Backend
```

复制环境文件：

```bash
cp deploy/.env.example deploy/.env
```

编辑：

```bash
vi deploy/.env
```

至少要改这些值：

```env
DASHSCOPE_API_KEY=你的真实值
ALIYUN_ANTHROPIC_API_KEY=你的真实值
AGENT_DATA_HOST_ROOT=/root/Backend/Agent-Do/data
```

如果要 Workshop 生成公网 URL，再补：

```env
OSS_ACCESS_KEY_ID=
OSS_ACCESS_KEY_SECRET=
OSS_BUCKET_NAME=
OSS_ENDPOINT=
OSS_DOMAIN=
```

## 启动

给脚本加执行权限：

```bash
chmod +x deploy/up.sh deploy/down.sh
```

启动核心后端：

```bash
./deploy/up.sh
```

如果还要带新闻抓取 worker：

```bash
./deploy/up.sh --profile crawler
```

如果还要带 EduRepo 后端：

```bash
./deploy/up.sh --profile edurepo
```

如果都要：

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

## 端口

- `8000` `crawl-api`
- `6670` `crawl-monitor`
- `18000` `agent-do`
- `5000` `workshop`
- `9010` `edurepo-backend` 仅在 `--profile edurepo` 时启动
