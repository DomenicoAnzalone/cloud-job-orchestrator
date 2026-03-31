#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAIN_TEMPLATE="${SCRIPT_DIR}/bicep/main.bicep"
DEFAULT_PARAMS_FILE="${SCRIPT_DIR}/bicep/parameters.json"

usage() {
  cat <<USAGE
Usage:
  $(basename "$0") -g <resource-group> -l <location> -p <name-prefix> [options]

Required:
  -g, --resource-group   Resource group name
  -l, --location         Azure region (e.g. eastus)
  -p, --prefix           Naming prefix (3-12 chars, lowercase letters/numbers)

Optional:
  -e, --environment      Environment label (default: dev)
      --subscription     Azure subscription ID or name
      --params           Parameters file path (default: infra/bicep/parameters.json)
      --what-if          Run ARM what-if before deployment
  -h, --help             Show this message
USAGE
}

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

check_az_login() {
  if ! az account show >/dev/null 2>&1; then
    fail "Azure CLI is not logged in. Run: az login"
  fi
}

RESOURCE_GROUP=""
LOCATION=""
PREFIX=""
ENVIRONMENT="dev"
SUBSCRIPTION=""
PARAMS_FILE="${DEFAULT_PARAMS_FILE}"
RUN_WHAT_IF="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -g|--resource-group)
      RESOURCE_GROUP="$2"
      shift 2
      ;;
    -l|--location)
      LOCATION="$2"
      shift 2
      ;;
    -p|--prefix)
      PREFIX="$2"
      shift 2
      ;;
    -e|--environment)
      ENVIRONMENT="$2"
      shift 2
      ;;
    --subscription)
      SUBSCRIPTION="$2"
      shift 2
      ;;
    --params)
      PARAMS_FILE="$2"
      shift 2
      ;;
    --what-if)
      RUN_WHAT_IF="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "Unknown argument: $1"
      ;;
  esac
done

[[ -n "$RESOURCE_GROUP" ]] || fail "Missing required argument: --resource-group"
[[ -n "$LOCATION" ]] || fail "Missing required argument: --location"
[[ -n "$PREFIX" ]] || fail "Missing required argument: --prefix"
[[ "$PREFIX" =~ ^[a-z0-9]{3,12}$ ]] || fail "--prefix must match ^[a-z0-9]{3,12}$"
[[ "$ENVIRONMENT" =~ ^[a-z0-9-]{2,15}$ ]] || fail "--environment must match ^[a-z0-9-]{2,15}$"
[[ -f "$MAIN_TEMPLATE" ]] || fail "Bicep template not found: $MAIN_TEMPLATE"
[[ -f "$PARAMS_FILE" ]] || fail "Parameters file not found: $PARAMS_FILE"

require_cmd az
require_cmd jq
check_az_login

if [[ -n "$SUBSCRIPTION" ]]; then
  echo "Setting Azure subscription: $SUBSCRIPTION"
  az account set --subscription "$SUBSCRIPTION" >/dev/null
fi

ACTIVE_SUB="$(az account show --query id -o tsv)"
echo "Active subscription: $ACTIVE_SUB"

echo "Creating/updating resource group: $RESOURCE_GROUP ($LOCATION)"
az group create --name "$RESOURCE_GROUP" --location "$LOCATION" >/dev/null

DEPLOYMENT_NAME="cjo-${ENVIRONMENT}-$(date +%Y%m%d%H%M%S)"

echo "Validating deployment..."
az deployment group validate \
  --resource-group "$RESOURCE_GROUP" \
  --name "${DEPLOYMENT_NAME}-validate" \
  --template-file "$MAIN_TEMPLATE" \
  --parameters "@$PARAMS_FILE" \
  --parameters environment="$ENVIRONMENT" location="$LOCATION" namePrefix="$PREFIX" >/dev/null

if [[ "$RUN_WHAT_IF" == "true" ]]; then
  echo "Running what-if..."
  az deployment group what-if \
    --resource-group "$RESOURCE_GROUP" \
    --name "${DEPLOYMENT_NAME}-whatif" \
    --template-file "$MAIN_TEMPLATE" \
    --parameters "@$PARAMS_FILE" \
    --parameters environment="$ENVIRONMENT" location="$LOCATION" namePrefix="$PREFIX"
fi

echo "Starting deployment: $DEPLOYMENT_NAME"
DEPLOY_OUTPUT="$(az deployment group create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$DEPLOYMENT_NAME" \
  --template-file "$MAIN_TEMPLATE" \
  --parameters "@$PARAMS_FILE" \
  --parameters environment="$ENVIRONMENT" location="$LOCATION" namePrefix="$PREFIX" \
  --query properties.outputs \
  -o json)"

echo

echo "Deployment completed successfully."

echo "$DEPLOY_OUTPUT" | jq . >/dev/null 2>&1 || fail "jq is required to render deployment outputs"

API_URL="$(echo "$DEPLOY_OUTPUT" | jq -r '.functionAppUrl.value')"
WEB_URL="$(echo "$DEPLOY_OUTPUT" | jq -r '.webAppUrl.value')"
FUNC_APP="$(echo "$DEPLOY_OUTPUT" | jq -r '.functionAppName.value')"
WEB_APP="$(echo "$DEPLOY_OUTPUT" | jq -r '.webAppName.value')"
STORAGE="$(echo "$DEPLOY_OUTPUT" | jq -r '.storageAccountName.value')"
SB_NS="$(echo "$DEPLOY_OUTPUT" | jq -r '.serviceBusNamespace.value')"
COSMOS="$(echo "$DEPLOY_OUTPUT" | jq -r '.cosmosAccountName.value')"
SIGNALR="$(echo "$DEPLOY_OUTPUT" | jq -r '.signalrName.value')"

echo "================ Deployment Outputs ================"
echo "Function App       : $FUNC_APP"
echo "Function API URL   : ${API_URL}/api"
echo "Web App            : $WEB_APP"
echo "Web App URL        : $WEB_URL"
echo "Storage Account    : $STORAGE"
echo "Service Bus NS     : $SB_NS"
echo "Cosmos DB Account  : $COSMOS"
echo "SignalR Service    : $SIGNALR"
echo "===================================================="

echo
cat <<NEXT_STEPS
Next steps:
1) Deploy backend and frontend code artifacts to the created apps.
2) Complete Entra ID app registrations and redirect URI configuration (see infra/README.md).
3) Update backend/local.settings.json and .env from examples using the output values.
4) Optionally run: infra/setup-env.sh --resource-group $RESOURCE_GROUP --function-app $FUNC_APP --web-app $WEB_APP
NEXT_STEPS
