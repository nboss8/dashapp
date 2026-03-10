FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV GUNICORN_BIND=0.0.0.0:8050
ENV GUNICORN_ACCESSLOG=-
ENV GUNICORN_ERRORLOG=-
ENV REPORT_CACHE_DIR=/app/cache

EXPOSE 8050

CMD ["gunicorn", "-c", "gunicorn.conf.py", "server:server"]
