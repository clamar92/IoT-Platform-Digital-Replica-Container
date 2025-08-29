"""
mqtt_client.py
---------------
Gestione client MQTT della Digital Replica.

- Sottoscrive ai topic telemetria dei device
- Inserisce i dati ricevuti in MongoDB (collezione `dati`)
- Pubblica comandi ai device su topic dedicati (QoS 1)
- Supporta riconfigurazione a caldo NON bloccante (reconnect_async)
- Evita dipendenze da Flask current_app nelle callback

Convenzioni topic:
- Telemetria (device→DR): iot/<DR_ID>/<device_id>/telemetry
- Comandi (DR→device):   iot/<DR_ID>/<device_id>/cmd
"""

import json
import logging
import socket
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt
from src.config.runtime_config import read_persisted_mqtt
from src.storage.db import dati_collection

log = logging.getLogger("dr.mqtt")


class DRMQTT:
    """
    Wrapper del client Paho-MQTT con gestione config e reconnect non bloccante.
    """

    def __init__(self, dr_id: str):
        self.dr_id = dr_id
        self.client: Optional[mqtt.Client] = None

        # Config iniziale (persistita su disco)
        cfg = read_persisted_mqtt() or {}
        self.username = cfg.get("username") or ""
        self.password = cfg.get("password") or ""
        self.host = cfg.get("host", "localhost")
        self.port = int(cfg.get("port", 1883))
        self.base_topic = cfg.get("base_topic", f"iot/{self.dr_id}")

        # Stato connessione
        self._connected = False

        # Lock per operazioni di start/stop
        self._lock = threading.RLock()

    # --------------------- Topic helpers ---------------------

    @property
    def telemetry_topic(self) -> str:
        # iot/<DR_ID or base>/+/telemetry
        return f"{self.base_topic}/+/telemetry"

    def cmd_topic(self, device_id: str) -> str:
        return f"{self.base_topic}/{device_id}/cmd"

    # --------------------- Callback MQTT ---------------------

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self._connected = True
            client.subscribe(self.telemetry_topic, qos=1)
            log.info("MQTT connected to %s:%s. Subscribed %s", self.host, self.port, self.telemetry_topic)
        else:
            self._connected = False
            log.error("MQTT connect failed rc=%s", rc)

    def _on_disconnect(self, client, userdata, rc, properties=None):
        self._connected = False
        if rc != 0:
            log.warning("MQTT unexpected disconnect (rc=%s)", rc)
        else:
            log.info("MQTT disconnected")

    def _on_message(self, client, userdata, msg):
        # Telemetria in ingresso: inserisce in Mongo
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

    # --------------------- API ---------------------

    def start(self) -> bool:
        """
        Inizializza e prova a connettersi al broker.
        Non alza eccezioni fatali: se fallisce, logga e lascia l'API HTTP operativa.
        Ritorna True se connesso, False altrimenti.
        """
        with self._lock:
            # se già esiste, fermo eventuale loop precedente
            if self.client is not None:
                try:
                    self.client.loop_stop()
                    self.client.disconnect()
                except Exception:
                    pass
                self.client = None
                self._connected = False

            self.client = mqtt.Client(client_id=f"{self.dr_id}-client", clean_session=True)
            if self.username or self.password:
                self.client.username_pw_set(self.username, self.password)

            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_message = self._on_message

            log.info("Connecting to MQTT %s:%s (base_topic=%s)", self.host, self.port, self.base_topic)

            # Retry con backoff; se fallisce, NON killiamo il processo
            delay = 0.5
            for attempt in range(1, 11):  # ~15s max
                try:
                    # timeout breve sul socket per evitare blocchi
                    socket.setdefaulttimeout(4.0)
                    self.client.connect(self.host, self.port, keepalive=60)
                    # loop in background
                    self.client.loop_start()
                    return True
                except Exception as e:
                    log.warning("MQTT connect failed (attempt %s): %s", attempt, e)
                    time.sleep(delay)
                    delay = min(delay * 1.5, 4.0)

            log.error("Cannot connect to MQTT at %s:%s — proceeding without MQTT", self.host, self.port)
            # Non connessi: niente loop_start, ma lasciamo l'oggetto pronto per retry futuri
            try:
                # paho richiede loop per callback anche se non connessi; lo avviamo comunque
                self.client.loop_start()
            except Exception:
                pass
            return False

    def stop(self):
        with self._lock:
            if self.client is not None:
                try:
                    self.client.loop_stop()
                    self.client.disconnect()
                except Exception:
                    pass
                self.client = None
                self._connected = False

    def is_connected(self) -> bool:
        return bool(self._connected)

    def restart_with(self, new_cfg: dict):
        """
        Aggiorna la configurazione e rilancia la connessione in modo NON bloccante.
        new_cfg può contenere: host, port, base_topic, username, password
        """
        host = new_cfg.get("host", self.host)
        port = int(new_cfg.get("port", self.port))
        base = new_cfg.get("base_topic", self.base_topic)
        user = new_cfg.get("username", self.username)
        pwd = new_cfg.get("password", self.password)

        # aggiorna subito i parametri locali
        self.host, self.port, self.base_topic = host, port, base
        self.username, self.password = user, pwd

        # reconnessione in background (risposta HTTP immediata)
        self.reconnect_async()

    def reconnect_async(self, attempts: int = 3, connect_timeout: float = 3.0):
        """
        Riavvia la connessione MQTT in background.
        - Non blocca il thread chiamante (es. la request /admin/mqtt).
        - Tenta 'attempts' volte con timeout breve.
        """
        def _reconnect():
            with self._lock:
                try:
                    if self.client is not None:
                        try:
                            self.client.loop_stop()
                            self.client.disconnect()
                        except Exception:
                            pass
                        self.client = None
                        self._connected = False

                    self.client = mqtt.Client(client_id=f"{self.dr_id}-client", clean_session=True)
                    if self.username or self.password:
                        self.client.username_pw_set(self.username, self.password)

                    self.client.on_connect = self._on_connect
                    self.client.on_disconnect = self._on_disconnect
                    self.client.on_message = self._on_message

                    for i in range(1, attempts + 1):
                        try:
                            socket.setdefaulttimeout(connect_timeout)
                            self.client.connect(self.host, self.port, keepalive=60)
                            self.client.loop_start()
                            log.info("Reconnected to MQTT %s:%s (attempt %s)", self.host, self.port, i)
                            return
                        except Exception as e:
                            log.warning("Reconnect attempt %s failed: %s", i, e)
                            time.sleep(1.0)

                    log.error("Failed to reconnect to MQTT at %s:%s", self.host, self.port)
                except Exception as e:
                    log.exception("Unexpected error during reconnect_async: %s", e)

        threading.Thread(target=_reconnect, name=f"mqtt-reconnect-{self.dr_id}", daemon=True).start()

    def publish_cmd(self, device_id: str, message: dict):
        """
        Pubblica un comando sul topic del device con QoS=1.
        Ritorna il topic pubblicato. Solleva RuntimeError se il client non è pronto.
        """
        if not self.client:
            raise RuntimeError("MQTT client not started")
        topic = self.cmd_topic(device_id)
        info = self.client.publish(topic, json.dumps(message), qos=1, retain=False)
        # Attende conferma publish (non blocca troppo)
        info.wait_for_publish(timeout=3.0)
        if not info.is_published():
            log.warning("Publish not confirmed on topic %s", topic)
        return topic
