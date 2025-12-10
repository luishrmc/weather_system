#!/bin/bash

# Define the output filename
TOKEN_FILE="../config/influxdb3/token.json"

# Run curl and redirect (>) the output to the file
# -s : Silent mode (hides the progress bar)
curl -s -X POST "http://localhost:8181/api/v3/configure/token/admin" \
  --header 'Accept: application/json' \
  --header 'Content-Type: application/json' \
  > "$TOKEN_FILE"

# Optional: Print a success message
echo "Request complete. Response saved to $TOKEN_FILE"
