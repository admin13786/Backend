
```markdown
# DeepSeek HTML 生成与 OSS 上传服务

这是一个基于 FastAPI 开发的轻量级后端服务，提供两大核心功能：使用 DeepSeek 大模型流式生成纯净的 HTML 代码，以及将文件直接上传至阿里云 OSS 并获取公网 URL。

## 目录结构

- `app.py`: FastAPI 主程序代码。
- `requirements.txt`: Python 依赖包列表。
- `.env` (需手动创建): 存储所有敏感的配置信息（OSS 密钥、API Key 等）。
- `.gitignore`: Git 忽略规则，防止密钥泄露。
- `s.yaml`: 阿里云 Serverless 函数计算 (FC) 部署配置文件。

## 快速开始 (本地运行)

### 1. 环境准备
请确保你的机器上已安装 Python 3.8 或以上版本。

### 2. 安装依赖
在项目根目录下，打开终端并运行以下命令：
```bash
pip install -r requirements.txt
```

### 3. 配置环境变量
在项目根目录下新建一个 `.env` 文件，并将以下内容复制进去，替换为你自己的真实密钥：

```env
# 阿里云 OSS 配置
OSS_ACCESS_KEY_ID="你的_OSS_ACCESS_KEY_ID"
OSS_ACCESS_KEY_SECRET="你的_OSS_ACCESS_KEY_SECRET"
OSS_BUCKET_NAME="你的_BUCKET_NAME" # 例如: eating-snake
OSS_ENDPOINT="你的_ENDPOINT"       # 例如: oss-cn-shenzhen.aliyuncs.com
OSS_DOMAIN="你的_自定义域名"        # 例如: [http://snake.fortuneai.cc](http://snake.fortuneai.cc)

# DeepSeek API 配置
DEEPSEEK_API_KEY="你的_DEEPSEEK_API_KEY"
```
> **⚠️ 警告:** `.env` 文件包含敏感凭证，绝不能被提交到公开的代码仓库中！

### 4. 启动服务
使用 Uvicorn 启动 FastAPI 服务：
```bash
uvicorn app:app --host 0.0.0.0 --port 9000 --reload
```
服务启动后，你可以直接在浏览器访问 Swagger 交互式接口文档界面：
 **http://127.0.0.1:9000/docs**

---

## 接口文档

### 1. 流式生成 HTML 代码
- **接口路径:** `/generate`
- **请求方式:** `POST`
- **功能描述:** 接收前端传来的系统提示词和用户上下文，通过 DeepSeek API 流式返回（Streaming）纯净的 HTML 代码，适合前端实现“打字机”效果。
- **请求数据格式 (application/json):**
  ```json
  {
    "context": "帮我写一个带蓝色背景的 HTML 按钮",
    "system_prompt": "你是一个前端开发专家"
  }
  ```
- **返回格式 (text/event-stream):** 逐块返回 HTML 纯文本数据。

### 2. 上传文件至 OSS
- **接口路径:** `/upload`
- **请求方式:** `POST`
- **功能描述:** 接收前端上传的文件（如通过 `/generate` 生成并打包好的 HTML 文件），直传至阿里云 OSS，并返回可以直接访问的 URL。
- **请求数据格式 (multipart/form-data):**
  - `file`: 上传的文件对象。
- **返回格式 (application/json):**
  ```json
  {
    "url": "[http://snake.fortuneai.cc/你的文件名.html](http://snake.fortuneai.cc/你的文件名.html)"
  }
  ```

---

## 前端调用标准流程建议

1. 前端先调用 `/generate` 接口，将大模型流式生成的 HTML 实时渲染给用户预览。
2. 预览生成完毕后，前端在本地将完整的 HTML 字符串封装成 File/Blob 对象。
3. 前端调用 `/upload` 接口，将该文件静默上传，拿到最终的 OSS 公网 `url` 用于分享或存储到数据库。

## 服务器部署说明

如果要在 Linux 服务器后台常驻运行，建议使用 `nohup` 或其他进程管理工具（如 Supervisor、Systemd）：
```bash
nohup uvicorn app:app --host 0.0.0.0 --port 9000 > app.log 2>&1 &
```
如果部署到阿里云函数计算 (FC)，可以直接使用项目自带的 `s.yaml` 通过 Serverless Devs 进行一键部署（需在云端控制台或 s.yaml 中配置好对应的环境变量）。

## Workshop Sandbox

- 每个 `conversationId` 现在都会绑定独立的 Docker 预览容器，作为第一层隔离。
- 每个工作区会写入 `.workshop-sandbox/policy.json` 和 `.workshop-sandbox/RULES.md`，作为第二层语言级限制。
- 预览默认优先走“构建后静态预览”，只有缺少 build 产物时才退回受控的 Node dev 预览。
- `opencode` 服务需要安装 `docker` CLI，并挂载 `/var/run/docker.sock`，同时加入 `WORKSHOP_SANDBOX_NETWORK` 网络。
- `WORKSHOP_SANDBOX_MAX_CONTAINERS` 用于限制容器池大小，超出后会优先回收最久未访问的对话容器。
- `WORKSHOP_SANDBOX_IDLE_TTL_MS` 用于控制空闲自动销毁时间；预览代理每次访问都会刷新活动时间。
