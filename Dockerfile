# Reddit Media Download Manager
# Bouwen:  docker build -t reddit-media-manager .
# Draaien: docker run -d --name reddit-media-manager -p 8000:8000 \
#             -v rmm-config:/config -v rmm-data:/data \
#             --restart unless-stopped reddit-media-manager
# (of gebruik docker-compose.yml)
#
# Mapping:
#   /config → app-state (settings.json, state.db, app.log)
#   /data   → gedownloade media (subreddit-mappen)
#
# Beveiliging: de container start als root, maar de entrypoint (docker-entrypoint.sh)
# repaireert het eigenaarschap van /config + /data en verlaagt daarna de
# privileges naar de niet-root user `rmm` (uid 1000) waarin de app draait.
# Dit lost het Docker named-volume-ownership quirk op zonder handmatig chown.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RMM_CONFIG=/config \
    RMM_DATAROOT=/data \
    RMM_USER=rmm \
    RMM_UID=1000

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
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
# Line-endings normaliseren (bron kan CRLF bevatten op Windows; een CR in de
# shebang breekt de script-executie) en executable maken.
RUN sed -i 's/\r$//' /usr/local/bin/docker-entrypoint.sh && chmod +x /usr/local/bin/docker-entrypoint.sh

# Niet-root app-user (deterministische uid 1000, consistent met bestaande volumes).
# De container start als root; de entrypoint chown-ent de volumes en verlaagt
# daarna de privileges naar rmm.
RUN useradd --uid 1000 --create-home rmm \
    && mkdir -p /config /data \
    && chown -R rmm:rmm /config /data

VOLUME /config
VOLUME /data
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import sys,urllib.request; r=urllib.request.urlopen('http://127.0.0.1:8000/api/status', timeout=4); sys.exit(0 if r.status==200 else 1)" || exit 1

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
