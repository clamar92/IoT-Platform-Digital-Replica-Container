"""
test_integration.py
-------------------
Test end-to-end per la Digital Replica in CI:
- Verifica /health
- Ingest HTTP → documento in Mongo
- Comando al device → messaggio MQTT ricevibile
- Telemetria via MQTT → documento in Mongo
- Admin GET/PUT db e mqtt (protetti da token)

Assume:
- App è su http://localhost:8000
- Mongo è su mongodb://localhost:27017/<db>
- MQTT broker è su localhost:1883
"""

import os
import time
import json
import threading

import pytest
import requests
import paho.mqtt.client as mqtt
from pymongo import MongoClient
import socket, pytest, os

MQTT_HOST = os.environ.get("MQTT_HOST", "test.mosquitto.org")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")
DR_TOKEN = os.environ.get("DR_TOKEN", "ci-token")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/digital_twin_db")
MQTT_BASE = os.environ.get("MQTT_BASE_TOPIC", "iot/dr-ci")
DR_ID = os.environ.get("DR_ID", "dr-ci")

DEVICE_ID = "dev-ci"

def test_health_ok():
    r = requests.get(f"{BASE_URL}/health", timeout=5)
    assert r.ok
    js = r.json()
    assert js["db"] is True
    assert js["mqtt"] is True

def test_ingest_http_and_mongo():
    payload = {"temp": 23.4, "hum": 47, "origin": "http-ci"}
    r = requests.post(
        f"{BASE_URL}/ingest/{DEVICE_ID}",
        headers={"Content-Type": "application/json", "X-DR-TOKEN": DR_TOKEN},
        data=json.dumps(payload),
        timeout=5,
    )
    assert r.ok and r.json()["status"] == "ok"

    # Check Mongo
    cli = MongoClient(MONGO_URI)
    db = cli.get_default_database()
    for _ in range(20):
        doc = db["dati"].find_one({"device_id": DEVICE_ID, "source": "http", "data.origin": "http-ci"})
        if doc:
            assert "ts" in doc
            break
        time.sleep(0.5)
    else:
        raise AssertionError("Ingest HTTP non trovato in Mongo")
    

def _mqtt_reachable(host, port, timeout=3):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False

MQTT_OK = _mqtt_reachable(MQTT_HOST, MQTT_PORT)

@pytest.mark.xfail(not MQTT_OK, reason="Public MQTT broker unreachable/flaky")
def test_command_mqtt_publish():
    # Subscribe to command topic to verify publish
    received = {}

    def on_msg(client, userdata, msg):
        received["topic"] = msg.topic
        received["payload"] = msg.payload.decode()

    cmd_topic = f"{MQTT_BASE}/{DEVICE_ID}/cmd"
    sub = mqtt.Client()
    sub.on_message = on_msg
    sub.connect(MQTT_HOST, MQTT_PORT, 60)
    sub.loop_start()
    sub.subscribe(cmd_topic, qos=1)

    # send command
    body = {"cmd": "set_threshold", "params": {"temp": 26}}
    r = requests.post(
        f"{BASE_URL}/devices/{DEVICE_ID}/commands",
        headers={"Content-Type": "application/json", "X-DR-TOKEN": DR_TOKEN},
        data=json.dumps(body),
        timeout=5,
    )
    assert r.ok and r.json()["status"] == "ok"

    # wait for message
    for _ in range(20):
        if "payload" in received:
            pl = json.loads(received["payload"])
            assert pl["cmd"] == "set_threshold"
            assert pl["params"]["temp"] == 26
            assert received["topic"] == cmd_topic
            break
        time.sleep(0.5)
    else:
        raise AssertionError("Comando MQTT non ricevuto dal subscriber di test")

    sub.loop_stop()
    sub.disconnect()

@pytest.mark.xfail(not MQTT_OK, reason="Public MQTT broker unreachable/flaky")
def test_mqtt_telemetry_to_mongo():
    # Publish telemetry like a device
    tele_topic = f"{MQTT_BASE}/{DEVICE_ID}/telemetry"
    pub = mqtt.Client()
    pub.connect(MQTT_HOST, MQTT_PORT, 60)
    pub.loop_start()
    payload = {"temp": 25.7, "hum": 49, "origin": "mqtt-ci"}
    pub.publish(tele_topic, json.dumps(payload), qos=1)
    pub.loop_stop()
    pub.disconnect()

    # Verify in Mongo
    cli = MongoClient(MONGO_URI)
    db = cli.get_default_database()
    for _ in range(30):
        doc = db["dati"].find_one({"device_id": DEVICE_ID, "source": "mqtt", "data.origin": "mqtt-ci"})
        if doc:
            assert "topic" in doc and doc["topic"] == tele_topic
            break
        time.sleep(0.5)
    else:
        raise AssertionError("Telemetria MQTT non trovata in Mongo")

def test_admin_db_and_mqtt_protected_and_persisted():
    # GET admin db (needs token)
    r = requests.get(f"{BASE_URL}/admin/db", headers={"X-DR-TOKEN": DR_TOKEN}, timeout=5)
    assert r.ok and "uri" in r.json()

    # PUT admin db -> switch to another name (persisted)
    new_uri = MONGO_URI.rsplit("/", 1)[0] + "/digital_twin_db_alt"
    r = requests.put(
        f"{BASE_URL}/admin/db",
        headers={"Content-Type": "application/json", "X-DR-TOKEN": DR_TOKEN},
        data=json.dumps({"uri": new_uri}),
        timeout=5,
    )
    assert r.ok and r.json()["status"] == "ok"

    # GET again must show the new value
    r = requests.get(f"{BASE_URL}/admin/db", headers={"X-DR-TOKEN": DR_TOKEN}, timeout=5)
    assert r.ok and r.json()["uri"].endswith("/digital_twin_db_alt")

    # PUT admin mqtt (host stays same in CI)
    r = requests.put(
        f"{BASE_URL}/admin/mqtt",
        headers={"Content-Type": "application/json", "X-DR-TOKEN": DR_TOKEN},
        data=json.dumps({"host": "mosquitto", "port": 1883}),
        timeout=5,
    )
    assert r.ok and r.json()["status"] == "ok"

    # GET admin mqtt reflects change
    r = requests.get(f"{BASE_URL}/admin/mqtt", headers={"X-DR-TOKEN": DR_TOKEN}, timeout=5)
    assert r.ok and r.json()["host"] in ("mosquitto", "localhost")
