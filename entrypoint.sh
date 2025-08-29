#!/usr/bin/env bash
# entrypoint.sh
# --------------
# Entrypoint del container Digital Replica.
# - Assicura che la directory /data esista
# - Avvia l'app Flask (app.py) in modalità standard
set -euo pipefail
mkdir -p "${DR_PERSIST_DIR:-/data}"
exec python app.py
