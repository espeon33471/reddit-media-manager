# Reddit Media Download Manager
# Bouwen:  docker build -t reddit-media-manager .
# Draaien: docker run -d --name reddit-media-manager -p 8000:8000 \
#             -v rmm-data:/data --restart unless-stopped reddit-media-manager
# (of gebruik docker-compose.yml)
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RMM_DATA=/data

WORKDIR /app

# ffmpeg voor video-conversie
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*

# Eerst dependencies (cache-lagen), daarna de code.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py example_sync.py ./
COPY rmm/ ./rmm/
COPY templates/ ./templates/
COPY static/ ./static/

# Draai niet als root; /data is de volume-map (state.db, app.log, media).
RUN useradd --create-home rmm \
    && mkdir -p /data \
    && chown -R rmm:rmm /data
USER rmm

VOLUME /data
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import sys,urllib.request; r=urllib.request.urlopen('http://127.0.0.1:8000/api/status', timeout=4); sys.exit(0 if r.status==200 else 1)" || exit 1

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
