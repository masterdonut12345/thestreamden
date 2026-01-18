FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=0

RUN apt-get update && apt-get install -y \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libdrm2 libgbm1 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 libxfixes3 \
    libcups2 libdbus-1-3 libxshmfence1 libx11-xcb1 libxss1 libasound2 \
    libglib2.0-0 libgtk-3-0 libpango-1.0-0 libpangocairo-1.0-0 \
    libx11-6 libxcb1 libxext6 libxrender1 fonts-liberation \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -r requirements.txt \
 && python -m playwright install chromium

CMD ["sh", "-c", "gunicorn -w 2 --bind 0.0.0.0:${PORT} wsgi:app"]
