"""
runtime_config.py
-----------------
Gestione della configurazione runtime della Digital Replica.

Funzioni principali:
- resolve_db_uri()       → ottiene/persist DB URI (MongoDB esterno)
- update_db_uri(uri)     → aggiorna DB URI e lo salva
- resolve_mqtt()         → ottiene/persist config MQTT (broker esterno)
- read_persisted_mqtt()  → legge la config MQTT da file
- update_mqtt(cfg)       → aggiorna parametri MQTT e li salva

Persistenza su volume:
- /data/db_uri.txt    (DB esterno)
- /data/mqtt.json     (broker esterno)

Ordine DB URI:
1. ENV MONGODB_URI/DB_URI
2. File persistito
3. Default locale (solo fallback di sviluppo)
"""

import os
import json
import pathlib
from typing import Optional, Dict, Any

# Directory di persistenza (volume del container)
PERSIST_DIR = pathlib.Path(os.getenv("DR_PERSIST_DIR", "/data"))
PERSIST_DIR.mkdir(parents=True, exist_ok=True)

DB_URI_FILE = PERSIST_DIR / "db_uri.txt"
MQTT_FILE = PERSIST_DIR / "mqtt.json"

# -----------------------------
# Utility file I/O
# -----------------------------
def _write_text(path: pathlib.Path, content: str) -> None:
    path.write_text((content or "").strip(), encoding="utf-8")

def _read_text(path: pathlib.Path) -> Optional[str]:
    if path.exists():
        s = path.read_text(encoding="utf-8").strip()
        return s or None
    return None

def _write_json(path: pathlib.Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj), encoding="utf-8")

def _read_json(path: pathlib.Path) -> Dict[str, Any]:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

# -----------------------------
# DB URI
# -----------------------------
def resolve_db_uri() -> str:
    """
    Ordine:
    1) ENV MONGODB_URI / DB_URI
    2) File persistito /data/db_uri.txt
    3) Default locale
    """
    env_uri = os.getenv("MONGODB_URI") or os.getenv("DB_URI")
    if env_uri:
        _write_text(DB_URI_FILE, env_uri)
        return env_uri

    persisted = _read_text(DB_URI_FILE)
    if persisted:
        return persisted

    # Fallback solo per sviluppo
    default = "mongodb://localhost:27017/digital_twin_db"
    _write_text(DB_URI_FILE, default)
    return default

def update_db_uri(new_uri: str) -> str:
    """
    Aggiorna e persiste il DB URI (usata dall'endpoint /admin/db).
    Non apre connessioni: serve solo a salvare il valore.
    """
    if not new_uri or not new_uri.strip():
        raise ValueError("Empty DB URI")
    _write_text(DB_URI_FILE, new_uri)
    return new_uri

# -----------------------------
# MQTT
# -----------------------------
def resolve_mqtt() -> Dict[str, Any]:
    """
    Crea/aggiorna la config MQTT a partire dagli ENV (se presenti),
    poi la persiste su /data/mqtt.json e la ritorna.
    Campi: host, port, username, password, base_topic
    """
    cfg = _read_json(MQTT_FILE)

    # ENV sovrascrivono se presenti
    host = os.getenv("MQTT_BROKER_HOST")
    port = os.getenv("MQTT_BROKER_PORT")
    user = os.getenv("MQTT_USERNAME")
    pwd = os.getenv("MQTT_PASSWORD")
    base = os.getenv("MQTT_BASE_TOPIC")

    if host is not None: cfg["host"] = host
    if port is not None: cfg["port"] = int(port)
    if user is not None: cfg["username"] = user
    if pwd  is not None: cfg["password"] = pwd
    if base is not None: cfg["base_topic"] = base

    # Default se mancanti
    if "host" not in cfg: cfg["host"] = "localhost"
    if "port" not in cfg: cfg["port"] = 1883
    if "username" not in cfg: cfg["username"] = ""
    if "password" not in cfg: cfg["password"] = ""
    if "base_topic" not in cfg:
        dr_id = os.getenv("DR_ID", "dr-001")
        cfg["base_topic"] = f"iot/{dr_id}"

    _write_json(MQTT_FILE, cfg)
    return cfg

def read_persisted_mqtt() -> Dict[str, Any]:
    """Ritorna la config MQTT dal file (se non esiste ancora, la crea dai default/ENV)."""
    cfg = _read_json(MQTT_FILE)
    if not cfg:
        cfg = resolve_mqtt()
    return cfg

def update_mqtt(new_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Aggiorna solo i campi forniti (host/port/username/password/base_topic),
    li persiste e ritorna la config completa.
    """
    cfg = read_persisted_mqtt()
    for k in ("host", "port", "username", "password", "base_topic"):
        if k in new_cfg and new_cfg[k] is not None:
            cfg[k] = int(new_cfg[k]) if k == "port" else new_cfg[k]
    _write_json(MQTT_FILE, cfg)
    return cfg


def read_persisted_db_uri() -> Optional[str]:
    """
    Ritorna l'URI DB leggendo SOLO dal file persistito (/data/db_uri.txt).
    None se il file non esiste ancora.
    """
    return _read_text(DB_URI_FILE)

def get_current_db_uri() -> str:
    """
    Usa SEMPRE la configurazione persistita se presente.
    Se non c'è ancora, inizializza con resolve_db_uri() (che può usare ENV una volta) e la persiste.
    """
    persisted = read_persisted_db_uri()
    if persisted:
        return persisted
    return resolve_db_uri()