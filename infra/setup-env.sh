#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage:
  $(basename "$0") --resource-group <rg> --function-app <func-app> --web-app <web-app>

Reads deployed app settings and prints/export-ready values for:
- backend/local.settings.json
- .env
USAGE
}

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

RG=""
FUNC_APP=""
WEB_APP=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --resource-group)
      RG="$2"; shift 2 ;;
    --function-app)
      FUNC_APP="$2"; shift 2 ;;
    --web-app)
      WEB_APP="$2"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      fail "Unknown argument: $1" ;;
  esac
done

[[ -n "$RG" ]] || fail "--resource-group is required"
[[ -n "$FUNC_APP" ]] || fail "--function-app is required"
[[ -n "$WEB_APP" ]] || fail "--web-app is required"

require_cmd az
require_cmd jq

az account show >/dev/null 2>&1 || fail "Azure CLI not logged in. Run az login"

FUNC_SETTINGS="$(az functionapp config appsettings list -g "$RG" -n "$FUNC_APP" -o json)"
WEB_SETTINGS="$(az webapp config appsettings list -g "$RG" -n "$WEB_APP" -o json)"

extract() {
  local json="$1"
  local key="$2"
  echo "$json" | jq -r --arg key "$key" '.[] | select(.name==$key) | .value' | head -n 1
}

cat <<ENV
# ---------- Suggested .env / local.settings values ----------
FUNCTIONS_WORKER_RUNTIME=$(extract "$FUNC_SETTINGS" "FUNCTIONS_WORKER_RUNTIME")
AzureWebJobsStorage=$(extract "$FUNC_SETTINGS" "AzureWebJobsStorage")
DEPLOYMENT_STORAGE_CONNECTION_STRING=$(extract "$FUNC_SETTINGS" "DEPLOYMENT_STORAGE_CONNECTION_STRING")
SERVICEBUS_CONNECTION=$(extract "$FUNC_SETTINGS" "SERVICEBUS_CONNECTION")
SERVICEBUS_JOBS_QUEUE=$(extract "$FUNC_SETTINGS" "SERVICEBUS_JOBS_QUEUE")
COSMOS_ENDPOINT=$(extract "$FUNC_SETTINGS" "COSMOS_ENDPOINT")
COSMOS_KEY=$(extract "$FUNC_SETTINGS" "COSMOS_KEY")
COSMOS_DB=$(extract "$FUNC_SETTINGS" "COSMOS_DB")
COSMOS_CONTAINER=$(extract "$FUNC_SETTINGS" "COSMOS_CONTAINER")
BLOB_CONNECTION=$(extract "$FUNC_SETTINGS" "BLOB_CONNECTION")
BLOB_ACCOUNT_URL=$(extract "$FUNC_SETTINGS" "BLOB_ACCOUNT_URL")
BLOB_INPUT_CONTAINER=$(extract "$FUNC_SETTINGS" "BLOB_INPUT_CONTAINER")
BLOB_OUTPUT_CONTAINER=$(extract "$FUNC_SETTINGS" "BLOB_OUTPUT_CONTAINER")
SIGNALR_CONNECTION_STRING=$(extract "$FUNC_SETTINGS" "SIGNALR_CONNECTION_STRING")
SIGNALR_HUB_NAME=$(extract "$FUNC_SETTINGS" "SIGNALR_HUB_NAME")
AZURE_AD_TENANT_ID=$(extract "$FUNC_SETTINGS" "AZURE_AD_TENANT_ID")
AZURE_AD_CLIENT_ID=$(extract "$FUNC_SETTINGS" "AZURE_AD_CLIENT_ID")
AZURE_AD_API_AUDIENCE=$(extract "$FUNC_SETTINGS" "AZURE_AD_API_AUDIENCE")
AZURE_AD_API_SCOPE=$(extract "$FUNC_SETTINGS" "AZURE_AD_API_SCOPE")
AZURE_AD_AUTHORITY=$(extract "$FUNC_SETTINGS" "AZURE_AD_AUTHORITY")

FRONTEND_API_BASE_URL=$(extract "$WEB_SETTINGS" "FRONTEND_API_BASE_URL")
API_BASE_URL=$(extract "$WEB_SETTINGS" "API_BASE_URL")
CJO_API_BASE_URL=$(extract "$WEB_SETTINGS" "CJO_API_BASE_URL")
PORT=$(extract "$WEB_SETTINGS" "PORT")
# ------------------------------------------------------------
ENV
