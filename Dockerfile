# DHT scraper image: the Python package only, standard library, unprivileged user, SQLite in /data.
FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DHT_DATABASE=/data/torrents.sqlite3

# An empty named volume mounted on /data takes this owner on first use.
RUN mkdir /data && chown 10001:10001 /data

WORKDIR /app
COPY dht_scraper/ ./dht_scraper/

USER 10001:10001
EXPOSE 8080/tcp 6881-6888/udp

HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --start-interval=1s \
    CMD ["python3", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/api/stats', timeout=2).read()"]

ENTRYPOINT ["python3", "-m", "dht_scraper", "--no-browser", "--web-host", "0.0.0.0"]

# Last, so a new revision only adds a metadata layer.
ARG REVISION=unknown
LABEL org.opencontainers.image.title="dht-scraper" \
      org.opencontainers.image.source="https://github.com/J4GL/LIF" \
      org.opencontainers.image.licenses="CC0-1.0" \
      org.opencontainers.image.revision="${REVISION}"
