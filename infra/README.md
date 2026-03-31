# Infrastructure - Cloud Job Orchestrator

## 1. Overview

This folder contains the Azure infrastructure provisioning flow for the project.

Relevant files:

* `infra/deploy.sh`: creates/updates the Resource Group, validates the Bicep template, optionally runs `what-if`, then deploys the infrastructure.
* `infra/bicep/main.bicep`: IaC definition of Azure resources.
* `infra/bicep/parameters.json` and `infra/bicep/parameters.test.json`: example parameter values.
* `infra/setup-env.sh`: reads app settings from Function App/Web App and prints useful variables for `.env` and `backend/local.settings.json`.

## 2. Prerequisites

Required Azure permissions:

* Permission to create resources in the target subscription/resource group.
* Permission to create role assignments (the template creates RBAC assignments).

Required tools (mandatory because used by scripts):

* `bash`
* `az` (Azure CLI)
* `jq`

Additional tools required for full application deployment:

* `zip` (to create deployable packages)
* `python3` + `pip` (backend Azure Functions)
* `node` + `npm` (frontend Node/Express)

## 3. Required tools installation

Install tools based on your OS. Verify they are available in PATH:

```bash
az version
jq --version
zip -v
python3 --version
npm --version
```

## 4. Azure authentication

Login and select the subscription:

```bash
az login
az account set --subscription "<subscription-id-or-name>"
az account show --query id -o tsv
```

## 5. Project configuration

### 5.1 Infrastructure parameters file

Create an environment-specific parameter file:

```bash
cp infra/bicep/parameters.json infra/bicep/parameters.<env>.json
```

Update at least:

* `environment`
* `location`
* `namePrefix`
* `azureAdTenantId`
* `azureAdClientId`
* `azureAdApiAudience`
* `azureAdApiScope`

Placeholders `<set-manually>` in parameters are not valid for production and must be replaced.

### 5.2 Backend local config

Create Functions local file:

```bash
cp backend/local.settings.example.json backend/local.settings.json
```

### 5.3 Frontend local config

Create local environment variables:

```bash
cp .env.example .env
```

## 6. Environment variables reference

The following variables are used by backend/frontend code or set by Bicep in app settings.

### Runtime/backend

* `FUNCTIONS_WORKER_RUNTIME`: Functions runtime (python).
* `AzureWebJobsStorage`: storage used by Functions runtime.
* `DEPLOYMENT_STORAGE_CONNECTION_STRING`: additional storage for deploy/artifacts.
* `SERVICEBUS_CONNECTION`: Service Bus connection used by triggers and producers.
* `SERVICEBUS_JOBS_QUEUE`: job queue (default `q-jobs`).
* `COSMOS_ENDPOINT`: Cosmos DB account endpoint.
* `COSMOS_KEY`: Cosmos DB account key.
* `COSMOS_DB`: Cosmos database (default `cjo`).
* `COSMOS_CONTAINER`: Cosmos container (default `jobs`).
* `BLOB_CONNECTION`: Blob Storage connection.
* `BLOB_ACCOUNT_URL`: blob account URL (used for SAS/output links).
* `BLOB_INPUT_CONTAINER`: input container (default `input`).
* `BLOB_OUTPUT_CONTAINER`: output container (default `output`).
* `OUTPUT_LINK_TTL_MINUTES`: output download link TTL.
* `SIGNALR_CONNECTION_STRING`: Azure SignalR connection.
* `SIGNALR_HUB_NAME`: SignalR hub (default `jobs`).
* `WORKER_DELAY_SECONDS`: artificial worker delay (demo/testing).

### Auth (Entra ID / Azure AD)

* `AZURE_AD_TENANT_ID`: tenant ID.
* `AZURE_AD_CLIENT_ID`: frontend app client ID used by MSAL.
* `AZURE_AD_API_AUDIENCE`: backend API audience (`api://...`).
* `AZURE_AD_API_SCOPE`: OAuth scope required by frontend.
* `AZURE_AD_AUTHORITY`: tenant login authority.

### Frontend

* `FRONTEND_API_BASE_URL`: API base URL.
* `API_BASE_URL`: API base URL compatibility alias.
* `CJO_API_BASE_URL`: API base URL compatibility alias.
* `PORT`: Express frontend server port.

## 7. Deployment

### 7.1 Validation / what-if (supported)

The script `infra/deploy.sh` always runs `validate` before deployment.
To simulate changes use `--what-if`.

Example:

```bash
chmod +x infra/deploy.sh infra/setup-env.sh

./infra/deploy.sh \
  --resource-group rg-cjo-dev \
  --location eastus \
  --prefix cjo \
  --environment dev \
  --params infra/bicep/parameters.dev.json \
  --what-if
```

### 7.2 Actual infrastructure deployment

Example without what-if:

```bash
./infra/deploy.sh \
  --resource-group rg-cjo-dev \
  --location eastus \
  --prefix cjo \
  --environment dev \
  --params infra/bicep/parameters.dev.json
```

The script creates/updates the Resource Group, then deploys `infra/bicep/main.bicep`.

### 7.3 Backend application deployment (Function App)

After provisioning, backend code must also be published (the template only creates infrastructure + app settings).

```bash
TMP_DIR="$(mktemp -d)"
BACKEND_ZIP="$TMP_DIR/backend.zip"

cd backend
zip -r "$BACKEND_ZIP" . -x "__pycache__/*" "*.pyc" ".venv/*"

az functionapp deployment source config-zip \
  --resource-group rg-cjo-dev \
  --name <function-app-name-output> \
  --src "$BACKEND_ZIP"

cd - >/dev/null
```

### 7.4 Frontend application deployment (Web App)

After provisioning, frontend code must also be published.

```bash
TMP_DIR="$(mktemp -d)"
FRONTEND_ZIP="$TMP_DIR/frontend.zip"

cd frontend
zip -r "$FRONTEND_ZIP" . -x "node_modules/*"

az webapp deploy \
  --resource-group rg-cjo-dev \
  --name <web-app-name-output> \
  --src-path "$FRONTEND_ZIP" \
  --type zip

cd - >/dev/null
```

## 8. `deploy.sh` parameters explanation

Supported parameters:

* `-g, --resource-group` (required): target Resource Group name.
* `-l, --location` (required): Azure region (e.g. `eastus`).
* `-p, --prefix` (required): naming prefix, regex `^[a-z0-9]{3,12}$`.
* `-e, --environment` (optional, default `dev`): environment label, regex `^[a-z0-9-]{2,15}$`.
* `--subscription` (optional): subscription ID or name.
* `--params` (optional): Bicep parameter file (default `infra/bicep/parameters.json`).
* `--what-if` (optional): runs ARM what-if before create.
* `-h, --help`: help.

## 9. Expected outputs

At the end of deployment, `infra/deploy.sh` prints:

* Function App name
* Function API URL (`.../api`)
* Web App name
* Web App URL
* Storage Account name
* Service Bus Namespace name
* Cosmos DB Account name
* SignalR Service name

These outputs are required for application deployment (`--name`) and final verification.

## 10. Verification

### 10.1 Azure app status check

```bash
az functionapp show -g rg-cjo-dev -n <function-app-name-output> --query "state" -o tsv
az webapp show -g rg-cjo-dev -n <web-app-name-output> --query "state" -o tsv
```

### 10.2 Backend API check

```bash
curl -i "https://<function-app-name-output>.azurewebsites.net/api/jobs/non-existent-id"
```

Expected result: HTTP response from endpoint (typically `404` if job does not exist).

### 10.3 Frontend check

Open in browser:

```text
https://<web-app-name-output>.azurewebsites.net
```

The web app should respond and serve the UI.

### 10.4 Extract real variables from app settings

```bash
./infra/setup-env.sh \
  --resource-group rg-cjo-dev \
  --function-app <function-app-name-output> \
  --web-app <web-app-name-output>
```

The script prints values to copy into `.env` and `backend/local.settings.json`.

## 11. Cleanup

To remove all created resources:

```bash
az group delete --name rg-cjo-dev --yes --no-wait
```

## 12. Important notes

* Required shell: `bash` (scripts use `#!/usr/bin/env bash` and `set -euo pipefail`).
* `jq` is mandatory: both `deploy.sh` and `setup-env.sh` use it directly.
* `deploy.sh` fails if `az account show` is not authenticated.
* Entra ID settings (`azureAd*`) are not auto-created: they must be manually set in the parameter file.
* The template sets `storage.location` to `norwayeast`; therefore storage does not follow the CLI `location` parameter.

## 13. Troubleshooting

Only issues observable from code/scripts:

* Error `--prefix must match ^[a-z0-9]{3,12}$`:
  use a lowercase alphanumeric prefix of length 3–12.

* Error `--environment must match ^[a-z0-9-]{2,15}$`:
  use lowercase environment with optional `-`, length 2–15.

* Error `Azure CLI is not logged in. Run: az login`:
  run `az login` again before `infra/deploy.sh` / `infra/setup-env.sh`.

* Error `jq is required ...` or `Required command not found: jq`:
  install `jq` and retry.

* Deployment auth/AAD runtime errors:
  verify consistency between `AZURE_AD_API_AUDIENCE`, `AZURE_AD_API_SCOPE`, `AZURE_AD_CLIENT_ID`, `AZURE_AD_TENANT_ID` and app registrations configured in the tenant.
