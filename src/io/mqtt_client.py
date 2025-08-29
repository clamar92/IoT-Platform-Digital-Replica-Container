"""
mqtt_client.py
---------------
Gestione client MQTT della Digital Replica.

- Sottoscrive ai topic telemetria dei device
- Inserisce i dati ricevuti in MongoDB (collezione `dati`)
- Pubblica comandi ai device su topic dedicati
- Può essere riavviato con nuova config (update via /admin/mqtt)

Convenzioni topic:
- Telemetria (device→DR): iot/<DR_ID>/<device_id>/telemetry
- Comandi (DR→device):   iot/<DR_ID>/<device_id>/cmd
"""

import time
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt
from src.config.runtime_config import read_persisted_mqtt
from src.storage.db import dati_collection

log = logging.getLogger("dr.mqtt")

class DRMQTT:
    def __init__(self, dr_id: str):
        self.dr_id = dr_id
        self.client: Optional[mqtt.Client] = None
        self.cfg = read_persisted_mqtt()

    @property
    def telemetry_topic(self) -> str:
        # iot/<DR_ID>/+/telemetry
        base = self.cfg.get("base_topic", f"iot/{self.dr_id}")
        return f"{base}/+/telemetry"

    def cmd_topic(self, device_id: str) -> str:
        base = self.cfg.get("base_topic", f"iot/{self.dr_id}")
        return f"{base}/{device_id}/cmd"

    # ----- MQTT callbacks (NO Flask current_app qui!) -----
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.subscribe(self.telemetry_topic, qos=1)
            log.info("MQTT connected. Subscribed %s", self.telemetry_topic)
        else:
            log.error("MQTT connect failed rc=%s", rc)

    def _on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode("utf-8")
            data = json.loads(payload)
        except Exception:
            data = {"raw": msg.payload.decode("utf-8", errors="ignore")}

        parts = msg.topic.split("/")
        device_id = parts[-2] if len(parts) >= 2 else "unknown"

        doc = {
            "ts": datetime.now(timezone.utc),
            "device_id": device_id,
            "source": "mqtt",
            "data": data,
            "topic": msg.topic,
            "dr_id": self.dr_id,
        }
        try:
            dati_collection().insert_one(doc)
        except Exception as e:
            log.exception("Failed to insert telemetry into Mongo: %s", e)

    # ----- API -----
    def start(self):
        cfg = self.cfg
        self.client = mqtt.Client(client_id=f"{self.dr_id}-client", clean_session=True)
        if cfg.get("username") or cfg.get("password"):
            self.client.username_pw_set(cfg.get("username") or "", cfg.get("password") or "")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

        host = cfg.get("host", "localhost")
        port = int(cfg.get("port", 1883))
        log.info("Connecting to MQTT %s:%s (base_topic=%s)", host, port, cfg.get("base_topic"))

        # --- retry con backoff esponenziale ---
        delay = 0.5
        for attempt in range(1, 16):  # 15 tentativi (~30s max)
            try:
                self.client.connect(host, port, keepalive=60)
                break
            except Exception as e:
                log.warning("MQTT connect failed (attempt %s): %s", attempt, e)
                time.sleep(delay)
                delay = min(delay * 1.5, 5.0)
        else:
            # dopo i retry, se ancora fallisce, alziamo
            raise RuntimeError(f"Cannot connect to MQTT at {host}:{port}")

        # crea thread interno gestito da paho
        self.client.loop_start()

    def restart_with(self, new_cfg: dict):
        # Ferma, aggiorna cfg, riavvia
        if self.client:
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                pass
        self.cfg.update(new_cfg)
        self.start()

    def publish_cmd(self, device_id: str, message: dict):
        if not self.client:
            raise RuntimeError("MQTT client not started")
        topic = self.cmd_topic(device_id)
        info = self.client.publish(topic, json.dumps(message), qos=1, retain=False)
        info.wait_for_publish(timeout=2.0)
        return topic
