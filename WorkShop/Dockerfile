# 基于 Python 3.11 轻量级镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

# 复制应用代码
COPY app.py .
COPY metrics.py .
COPY skill_store.py .

# 暴露端口
EXPOSE 5000

# 启动命令
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000"]
