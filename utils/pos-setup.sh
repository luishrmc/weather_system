#!/bin/bash

# 1. CONFIGURATION (Matching your Python class defaults)
HOST="localhost"
PORT="8181"
DB_NAME="weather_station"
TABLE_NAME="weather_data"
TOKEN_FILE="../config/influxdb3/token.json"

# 2. LOAD TOKEN
# Reads the "token" field from the JSON file created in the previous step
TOKEN=$(jq -r '.token' $TOKEN_FILE)

# Check if token was found
if [ -z "$TOKEN" ] || [ "$TOKEN" == "null" ]; then
  echo "Error: Could not read token from $TOKEN_FILE"
  exit 1
fi

echo "Using Token: $TOKEN"

# 3. CREATE DATABASE
# InfluxDB v3 requires explicit database creation via the Management API
echo "Creating database: $DB_NAME..."
curl -s -X POST "http://${HOST}:${PORT}/api/v3/databases" \
  --header "Authorization: Bearer $TOKEN" \
  --header 'Content-Type: application/json' \
  --data "{\"name\": \"$DB_NAME\"}"

# 4. CREATE TABLE (By writing 1 data point)
# Tables (Measurements) are "Schema-on-Write". 
# We write a dummy point to initialize the table.
echo -e "\nInitializing table: $TABLE_NAME..."

# Line Protocol Format: measurement,tag=value field=value
DATA="${TABLE_NAME},init=true status=\"created\""

curl -s -X POST "http://${HOST}:${PORT}/api/v3/write?database=${DB_NAME}" \
  --header "Authorization: Bearer $TOKEN" \
  --data-binary "$DATA"

echo -e "\nDone. Database '$DB_NAME' and Table '$TABLE_NAME' are ready."
