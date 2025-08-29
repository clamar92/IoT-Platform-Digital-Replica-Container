# Dockerfile
# ----------
# Costruisce l'immagine Docker della Digital Replica.
# - Copia codice e requirements
# - Installa dipendenze
# - Imposta /data come volume persistente per DB/MQTT config
# - Espone porta 8000
# - Usa entrypoint.sh come comando di avvio

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# tini per gestione segnali corretta
RUN useradd -m appuser && apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates tini && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Persistenza configurazioni runtime
RUN mkdir -p /data && chown -R appuser:appuser /data
VOLUME ["/data"]

EXPOSE 8000

# Variabili consigliate
ENV DR_PERSIST_DIR=/data \
    DR_CONFIG_FILE=/app/config/database.yaml

# Entrypoint
COPY entrypoint.sh /usr/local/bin/entrypoint
RUN chmod +x /usr/local/bin/entrypoint

USER appuser
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/entrypoint"]
