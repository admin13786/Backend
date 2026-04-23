ARG PYTHON_IMAGE=python:3.10-slim
FROM ${PYTHON_IMAGE}

ARG APT_MIRROR_HOST=https://mirrors.aliyun.com
ARG PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple
ARG INSTALL_CJK_FONT=false

WORKDIR /app

RUN if [ -n "${APT_MIRROR_HOST}" ]; then \
      if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
        sed -i "s|http://deb.debian.org|${APT_MIRROR_HOST}|g; s|https://deb.debian.org|${APT_MIRROR_HOST}|g; s|http://security.debian.org|${APT_MIRROR_HOST}|g; s|https://security.debian.org|${APT_MIRROR_HOST}|g" /etc/apt/sources.list.d/debian.sources; \
      fi; \
    fi && \
    apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates && \
    if [ "${INSTALL_CJK_FONT}" = "true" ]; then \
      apt-get install -y --no-install-recommends fonts-noto-cjk; \
    fi && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install \
    --no-cache-dir \
    --retries 10 \
    --timeout 120 \
    -i ${PIP_INDEX_URL} \
    -r requirements.txt

COPY . .

RUN mkdir -p /app/data

ENV PYTHONUNBUFFERED=1
ENV EDU_REPO_DATA_DIR=/app/data

EXPOSE 9010

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "9010"]
