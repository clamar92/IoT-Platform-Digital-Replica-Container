"""
db.py
------
Gestione connessione al database NoSQL (MongoDB esterno) e bootstrap iniziale.

Obiettivi:
- Supportare URI SENZA nome DB (es. "mongodb://localhost:27017") usando un nome di default.
- Creare automaticamente database/collezioni all'avvio, se non esistono.
- Esporre handle comodi:
    - get_client()           → MongoClient singleton
    - get_db()               → database (da URI oppure da env DB_NAME, default "digital_twin_db")
    - dati_collection()      → collezione `dati` con indice (device_id, ts)
    - ensure_initialized()   → forza la creazione delle collezioni/indici
    - rebind_after_config_change() → reset connessioni dopo cambio URI via /admin/db

Note:
- In Mongo, DB/collection vengono creati al primo insert. Qui forziamo esplicitamente la creazione
  per avere un comportamento prevedibile anche se l'app parte "a freddo".
"""

import os
from typing import Optional
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.errors import CollectionInvalid
from src.config.runtime_config import get_current_db_uri

_client: Optional[MongoClient] = None
_db = None
_dati: Optional[Collection] = None

def _pick_db_name() -> str:
    """
    Se l'URI non specifica il DB (niente path), usa:
    - env DB_NAME (se presente)
    - altrimenti "digital_twin_db"
    """
    name = os.getenv("DB_NAME")
    if name and name.strip():
        return name.strip()
    return "digital_twin_db"

def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(get_current_db_uri())
    return _client

def get_db():
    global _db
    if _db is None:
        cli = get_client()
        # Se l'URI non ha DB nella path, get_default_database() è None → scegliamo noi
        db = cli.get_default_database()
        if db is None:
            db = cli[_pick_db_name()]
        _db = db
    return _db

def _ensure_collection(name: str) -> Collection:
    db = get_db()
    if name in db.list_collection_names():
        return db.get_collection(name)
    # crea in modo esplicito (anche un create+index va bene)
    try:
        db.create_collection(name)
    except CollectionInvalid:
        # race benigna: se la crea un altro thread nel frattempo
        pass
    return db.get_collection(name)

def dati_collection() -> Collection:
    global _dati
    if _dati is None:
        col = _ensure_collection("dati")
        # indice utile per query su device e tempo
        col.create_index([("device_id", ASCENDING), ("ts", ASCENDING)])
        _dati = col
    return _dati

def ensure_initialized():
    """
    Forza la creazione delle collezioni fondamentali e dei relativi indici.
    Da chiamare all'avvio dell'app (non indispensabile, ma rende esplicito il bootstrap).
    """
    _ = dati_collection()  # crea `dati` + indice
    # opzionale: anche la collezione "commands"
    cmds = _ensure_collection("commands")
    cmds.create_index([("device_id", ASCENDING), ("_id", ASCENDING)])

def rebind_after_config_change():
    """Usare dopo update DB URI per riaprire connessioni con i nuovi parametri."""
    global _client, _db, _dati
    if _client is not None:
        try:
            _client.close()
        except Exception:
            pass
    _client = None
    _db = None
    _dati = None
