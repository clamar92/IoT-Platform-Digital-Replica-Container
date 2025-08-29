<#
run-local.ps1
--------------
Sets all env vars for a local test run of the Digital Replica and starts the app.
Only affects this PowerShell process (nothing is saved system-wide).

To run test:
.\scripts\run-local.ps1

#>

# ------------ Required IDs & security ------------
$env:DR_ID    = "dr-001"
$env:DR_TOKEN = "supersegreto"   # change or leave empty to disable token checks

# ------------ Database (external Mongo) ------------
# If you only have a server URI without db name:
# $env:MONGODB_URI = "mongodb://localhost:27017"
# and choose the db name:
# $env:DB_NAME     = "digital_twin_db"

# If you already include a db name in the URI, set just this:
$env:MONGODB_URI = "mongodb://localhost:27017/digital_twin_db"

# ------------ MQTT broker (external) ------------
$env:MQTT_BROKER_HOST = "test.mosquitto.org"
$env:MQTT_BROKER_PORT = "1883"
$env:MQTT_USERNAME    = ""   # optional
$env:MQTT_PASSWORD    = ""   # optional
$env:MQTT_BASE_TOPIC  = "iot/dr-001"

# ------------ Optional: where to persist runtime config (db_uri.txt, mqtt.json) ------------
$env:DR_PERSIST_DIR = "$PWD\data"  # creates ./data on first run

# ------------ Start the app ------------
Write-Host "Starting Digital Replica with:"
Write-Host " DR_ID=$env:DR_ID"
Write-Host " MONGODB_URI=$env:MONGODB_URI  (DB_NAME=$env:DB_NAME)"
Write-Host " MQTT=$env:MQTT_BROKER_HOST:$env:MQTT_BROKER_PORT  base=$env:MQTT_BASE_TOPIC"
Write-Host " DR_PERSIST_DIR=$env:DR_PERSIST_DIR"

python app.py


