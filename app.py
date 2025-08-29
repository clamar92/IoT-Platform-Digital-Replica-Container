"""
app.py
-------
Entry point principale della Digital Replica.

- Crea e configura l'app Flask
- Inizializza il client MQTT (sottoscrizione telemetria, publish comandi)
- Registra le route REST (health, ingest, command, admin)
- Espone la Digital Replica come servizio HTTP (porta 8000)

Uso:
  python app.py     # avvio sviluppo
  # In container viene lanciato da entrypoint.sh
"""

import os
from flask import Flask
from src.api.routes import bp
from src.io.mqtt_client import DRMQTT
import logging
from src.config.runtime_config import get_current_db_uri, resolve_mqtt
from src.storage.db import ensure_initialized

logging.basicConfig(level=logging.INFO)

def create_app():
    app = Flask(__name__)

    # Identità e sicurezza base
    app.config["DR_ID"] = os.getenv("DR_ID", "dr-001")
    app.config["DR_TOKEN"] = os.getenv("DR_TOKEN", "")

    # Prime config: inizializza i file se non esistono ancora
    _ = get_current_db_uri()  # crea /data/db_uri.txt se manca
    _ = resolve_mqtt()        # crea /data/mqtt.json se manca

    # Avvio client MQTT (telemetria in → DB, comandi out → device)
    mqtt_obj = DRMQTT(dr_id=app.config["DR_ID"])
    mqtt_obj.start()
    app.config["MQTT_OBJ"] = mqtt_obj

    # Blueprint REST
    app.register_blueprint(bp)
    ensure_initialized()
    return app

app = create_app()

if __name__ == "__main__":
    # Avvio sviluppo; in produzione usare gunicorn (oppure mantenere così per semplicità)
    app.run(host="0.0.0.0", port=8000)
