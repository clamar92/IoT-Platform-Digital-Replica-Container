"""
routes.py
----------
API REST della Digital Replica.

Endpoint principali:
- GET  /health                → stato DB + MQTT
- POST /devices/<id>/commands → invia comando a device via MQTT
- POST /ingest/<id>           → ingest telemetria via HTTP (fallback)
- GET/PUT /admin/db           → leggi/aggiorna DB URI (esterno)
- GET/PUT /admin/mqtt         → leggi/aggiorna config MQTT (broker esterno)

Sicurezza:
- Se impostato DR_TOKEN, le API mutanti richiedono header: X-DR-TOKEN: <token>
"""

from datetime import datetime, timezone
from flask import Blueprint, request, jsonify, current_app
from src.storage.db import dati_collection, rebind_after_config_change, get_db
from src.config.runtime_config import (
    read_persisted_db_uri, update_db_uri,
    update_mqtt, read_persisted_mqtt, get_current_db_uri
)

bp = Blueprint("api", __name__)

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _require_token():
    token_conf = current_app.config.get("DR_TOKEN", "")
    if not token_conf:
        return None
    token = request.headers.get("X-DR-TOKEN")
    if token != token_conf:
        return jsonify({"status": "forbidden"}), 403
    return None

@bp.get("/health")
def health():
    # DB ping
    try:
        get_db().command("ping")
        db_ok = True
    except Exception:
        db_ok = False

    mqtt_obj = current_app.config.get("MQTT_OBJ")
    mqtt_ok = bool(mqtt_obj and mqtt_obj.is_connected())

    return jsonify({
        "status": "ok" if db_ok and mqtt_ok else "degraded",
        "db": db_ok,
        "mqtt": mqtt_ok,
        "dr_id": current_app.config.get("DR_ID"),
        "time": _now_iso(),
    })

@bp.post("/devices/<device_id>/commands")
def post_command(device_id: str):
    forbidden = _require_token()
    if forbidden:
        return forbidden

    body = request.get_json(silent=True) or {}
    cmd = body.get("cmd")
    params = body.get("params", {})
    if not cmd:
        return jsonify({"status": "error", "detail": "Missing 'cmd'"}), 400

    msg = {
        "cmd": cmd,
        "params": params,
        "ts": _now_iso(),
        "dr_id": current_app.config.get("DR_ID"),
    }

    mqtt_obj = current_app.config.get("MQTT_OBJ")
    if not mqtt_obj:
        return jsonify({"status": "error", "detail": "MQTT not initialized"}), 503

    topic = mqtt_obj.publish_cmd(device_id, msg)

    # Log comandi (facoltativo)
    try:
        get_db().get_collection("commands").insert_one(
            {"device_id": device_id, "command": msg, "topic": topic}
        )
    except Exception:
        # non bloccare l'API se il logging fallisce
        pass

    return jsonify({"status": "ok", "topic": topic, "command": msg})

@bp.post("/ingest/<device_id>")
def ingest_http(device_id: str):
    forbidden = _require_token()
    if forbidden:
        return forbidden

    payload = request.get_json(silent=True)
    if payload is None:
        return jsonify({"status": "error", "detail": "JSON body required"}), 400

    doc = {
        "ts": datetime.now(timezone.utc),
        "device_id": device_id,
        "source": "http",
        "data": payload,
        "dr_id": current_app.config.get("DR_ID"),
    }
    dati_collection().insert_one(doc)
    return jsonify({"status": "ok"})

# ---- Admin: DB esterno ----
@bp.get("/admin/db")
def get_db_uri():
    forbidden = _require_token()
    if forbidden:
        return forbidden

    uri = read_persisted_db_uri()
    if not uri:
        # primo avvio: inizializza file usando la risoluzione standard
        uri = get_current_db_uri()
    return jsonify({"uri": uri})

@bp.put("/admin/db")
def put_db_uri():
    forbidden = _require_token()
    if forbidden:
        return forbidden

    payload = request.get_json(silent=True) or {}
    new_uri = payload.get("uri")
    try:
        update_db_uri(new_uri)           # scrive /data/db_uri.txt
        rebind_after_config_change()     # riapre le connessioni con la nuova URI
        return jsonify({"status": "ok", "uri": new_uri})
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 400

# ---- Admin: MQTT esterno ----
@bp.get("/admin/mqtt")
def get_mqtt():
    forbidden = _require_token()
    if forbidden:
        return forbidden

    # Config persistita su disco
    persisted = read_persisted_mqtt() or {}

    # Sovrapponi runtime, così mostri i valori effettivi
    mqtt_obj = current_app.config.get("MQTT_OBJ")
    runtime = {}
    if mqtt_obj:
        runtime = {
            "host": mqtt_obj.host,
            "port": mqtt_obj.port,
            "base_topic": mqtt_obj.base_topic,
            "username": mqtt_obj.username or None,
            # NON esporre password: dai solo un flag
            "has_password": bool(mqtt_obj.password),
        }

    # merge semplice (runtime vince)
    out = {**persisted, **runtime}
    # pulizia: assicurati dei tipi corretti
    if "port" in out:
        out["port"] = int(out["port"])
    # rimuovi eventuale password persistita dai file, se c'è
    out.pop("password", None)

    return jsonify(out)

@bp.put("/admin/mqtt")
def put_mqtt():
    forbidden = _require_token()
    if forbidden:
        return forbidden

    payload = request.get_json(silent=True) or {}
    # Normalizza i campi accettati
    new_cfg = {
        k: v for k, v in payload.items()
        if k in {"host", "port", "base_topic", "username", "password"}
    }
    if "port" in new_cfg:
        try:
            new_cfg["port"] = int(new_cfg["port"])
        except Exception:
            return jsonify({"status": "error", "detail": "port must be integer"}), 400

    # 1) persisti subito su disco
    saved_cfg = update_mqtt(new_cfg)  # ritorna la cfg risultante (merge con esistente)

    # 2) aggiorna oggetto runtime e riconnetti in background (non blocca la request)
    mqtt_obj = current_app.config.get("MQTT_OBJ")
    if mqtt_obj:
        mqtt_obj.restart_with(saved_cfg)   # internamente fa reconnect_async

    # 3) risposta piatta senza password
    resp = {
        "status": "ok",
        "host": saved_cfg.get("host"),
        "port": int(saved_cfg.get("port", 1883)),
        "base_topic": saved_cfg.get("base_topic"),
        "username": saved_cfg.get("username") or None,
        "has_password": bool(saved_cfg.get("password")),
    }
    return jsonify(resp)
