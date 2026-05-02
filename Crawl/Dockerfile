# 使用 Python 3.10 基础镜像
ARG PYTHON_IMAGE=python:3.10-slim
FROM ${PYTHON_IMAGE}

ARG APT_MIRROR=https://mirrors.tuna.tsinghua.edu.cn
ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ARG PLAYWRIGHT_DOWNLOAD_HOST=

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN if [ -f /etc/apt/sources.list ]; then \
        sed -i "s|http://deb.debian.org|${APT_MIRROR}|g; s|https://deb.debian.org|${APT_MIRROR}|g; s|http://security.debian.org|${APT_MIRROR}|g; s|https://security.debian.org|${APT_MIRROR}|g" /etc/apt/sources.list; \
    fi && \
    if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
        sed -i "s|http://deb.debian.org|${APT_MIRROR}|g; s|https://deb.debian.org|${APT_MIRROR}|g; s|http://security.debian.org|${APT_MIRROR}|g; s|https://security.debian.org|${APT_MIRROR}|g" /etc/apt/sources.list.d/debian.sources; \
    fi && \
    apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libwayland-client0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# 创建日志目录
RUN mkdir -p /app/logs /app/static/page-shots

# 复制依赖文件
COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --no-cache-dir -i "${PIP_INDEX_URL}" -r requirements.txt

# 安装 Playwright 浏览器（用于 /api/page-screenshot）
# 默认走官方源；如果显式指定镜像源，则失败时自动回退到官方源。
RUN set -eux; \
    if [ -n "${PLAYWRIGHT_DOWNLOAD_HOST}" ]; then \
      PLAYWRIGHT_DOWNLOAD_HOST="${PLAYWRIGHT_DOWNLOAD_HOST}" python -m playwright install chromium || \
      python -m playwright install chromium; \
    else \
      python -m playwright install chromium; \
    fi

# 复制应用代码
COPY . .

# 设置环境变量
ENV PYTHONUNBUFFERED=1

# API 服务启动命令（包含 /api/page-screenshot 等接口）
EXPOSE 8000
CMD ["uvicorn", "run_local:app", "--host", "0.0.0.0", "--port", "8000"]
