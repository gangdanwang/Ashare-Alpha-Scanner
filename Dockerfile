FROM python:3.11-slim

WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY Ashare.py MonthLow.py stock_cache.py db.py ./
COPY web/ ./web/

# 环境变量默认值（可通过 -e 或 --env-file 覆盖）
ENV PYTHONUNBUFFERED=1 \
    FLASK_ENV=production

EXPOSE 5001

CMD ["python", "web/app.py"]
