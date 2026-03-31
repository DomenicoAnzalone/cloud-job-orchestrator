# Infrastructure Provisioning (Azure) - Cloud Job Orchestrator

## 1. Overview
This `/scripts` package provisions a production-ready baseline for the Cloud Job Orchestrator stack on Azure using **Bicep + Azure CLI**. It creates core infrastructure, configures application settings for backend/frontend apps, enables system-assigned managed identities, and applies role assignments.

### What gets provisioned
- Resource Group (created by bootstrap script)
- Storage Account + blob containers: `input`, `output`
- Service Bus Namespace + queue: `q-jobs`
- Cosmos DB (SQL API) + DB `cjo` + container `jobs`
- Azure Function App (Python backend)
- Function App plan (Consumption, Linux)
- App Service Plan (frontend)
- Web App (frontend)
- Application Insights
- Azure SignalR Service
- Managed identity + RBAC assignments

---

## 2. Prerequisites
- Azure subscription with rights to create resources and role assignments
  - Minimum practical role: **Contributor** on target subscription/resource group
  - Plus permission to assign roles (`Microsoft.Authorization/roleAssignments/write`) — typically **Owner** or **User Access Administrator**
- [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) 2.55+
- `jq` installed locally (used by scripts)
- Logged in to Azure CLI:
  ```bash
  az login
  az account set --subscription "<subscription-id-or-name>"
  ```

---

## 3. Quick Start (step-by-step)
1. Copy and edit deployment parameters:
   ```bash
   cp scripts/parameters.json scripts/parameters.<env>.json
   ```
2. Update `scripts/parameters.<env>.json` values:
   - `namePrefix` (lowercase alphanumeric, 3-12 chars)
   - `location`
   - Entra placeholders:
     - `azureAdTenantId`
     - `azureAdClientId`
     - `azureAdApiAudience`
     - `azureAdApiScope`
3. Run deployment:
   ```bash
   chmod +x scripts/deploy.sh scripts/setup-env.sh
   ./scripts/deploy.sh \
     --resource-group rg-cjo-dev \
     --location eastus \
     --prefix cjo \
     --environment dev \
     --params scripts/parameters.<env>.json
   ```
4. (Optional) Generate environment-variable output from live app settings:
   ```bash
   ./scripts/setup-env.sh \
     --resource-group rg-cjo-dev \
     --function-app <function-app-name> \
     --web-app <web-app-name>
   ```
5. Deploy application code (backend/frontend artifacts) to the created Function App and Web App.

---

## 4. Configuration
Use deployed outputs and app settings to populate local configuration files.

### `.env` (repo root)
Start from `.env.example` and set at minimum:
- Backend connectivity:
  - `SERVICEBUS_CONNECTION`
  - `SERVICEBUS_JOBS_QUEUE`
  - `COSMOS_ENDPOINT`
  - `COSMOS_KEY`
  - `COSMOS_DB=cjo`
  - `COSMOS_CONTAINER=jobs`
  - `BLOB_CONNECTION`
  - `BLOB_ACCOUNT_URL`
  - `BLOB_INPUT_CONTAINER=input`
  - `BLOB_OUTPUT_CONTAINER=output`
  - `SIGNALR_CONNECTION_STRING`
- Auth:
  - `AZURE_AD_TENANT_ID`
  - `AZURE_AD_CLIENT_ID`
  - `AZURE_AD_API_AUDIENCE`
  - `AZURE_AD_API_SCOPE`
  - `AZURE_AD_AUTHORITY`
- Frontend:
  - `FRONTEND_API_BASE_URL`
  - `API_BASE_URL`
  - `CJO_API_BASE_URL`

### `backend/local.settings.json`
Copy from `backend/local.settings.example.json` and set:
- `AzureWebJobsStorage`
- `DEPLOYMENT_STORAGE_CONNECTION_STRING`
- `SERVICEBUS_CONNECTION`
- `COSMOS_ENDPOINT` / `COSMOS_KEY`
- `BLOB_CONNECTION`
- `SIGNALR_CONNECTION_STRING`
- Azure AD values (`AZURE_AD_*`)

> Tip: `scripts/setup-env.sh` prints ready-to-copy values from deployed app settings.

---

## 5. IMPORTANT: Clear Separation of Steps

### ✅ Fully Automated
Handled by `deploy.sh` + `main.bicep`:
- Resource Group creation
- Storage account + `input/output` containers
- Service Bus namespace + `q-jobs` queue
- Cosmos DB account + SQL DB `cjo` + container `jobs`
- Function plan + Function App (system-assigned managed identity)
- App Service plan + Web App (system-assigned managed identity)
- Application Insights + app settings wiring
- SignalR Service + app settings wiring
- RBAC assignments:
  - Storage Blob Data Contributor
  - Cosmos DB Built-in Data Contributor (SQL role assignment)
  - Service Bus Data Sender + Receiver
  - SignalR App Server

### ⚠️ Manual One-Time Steps
Must be done once per tenant/application model:
- Entra ID app registrations (frontend + backend API)
- Expose API scope in backend app registration
- Configure frontend delegated API permissions
- Grant admin consent (if tenant requires)
- Set redirect URIs:
  - Local frontend URI (e.g. `http://localhost:3000`)
  - Web App URI (e.g. `https://<webapp>.azurewebsites.net`)
- Ensure backend audience/scope values match app registration:
  - `AZURE_AD_API_AUDIENCE`
  - `AZURE_AD_API_SCOPE`

Optional helper commands (review before use):
```bash
# Create frontend app registration
az ad app create --display-name "cjo-frontend" --sign-in-audience AzureADMyOrg

# Create backend API app registration
az ad app create --display-name "cjo-backend-api" --identifier-uris "api://<backend-app-id>"

# Add SPA redirect URI (frontend)
az ad app update --id <frontend-app-id> --web-redirect-uris "https://<webapp>.azurewebsites.net"
```

### 🔧 Tenant-Dependent Configuration
Depends on organization policy/security constraints:
- Whether local auth keys are allowed for Cosmos/Storage/ServiceBus/SignalR
- Naming rules and reserved prefixes
- Region restrictions and allowed SKUs
- Required private networking / firewall / vNet integration
- Mandatory tags, diagnostics, policy compliance
- Tenant consent workflow for API permissions

---

## 6. Outputs
`deploy.sh` prints key outputs after deployment:
- `Function API URL`
- `Web App URL`
- Resource names:
  - Function App
  - Web App
  - Storage Account
  - Service Bus Namespace
  - Cosmos DB Account
  - SignalR Service

You can also inspect ARM outputs directly:
```bash
az deployment group show \
  --resource-group <rg> \
  --name <deployment-name> \
  --query properties.outputs -o json
```

---

## 7. Troubleshooting

### `AuthorizationFailed` on role assignments
Your identity likely lacks role-assignment permissions. Use a principal with **Owner** or **User Access Administrator**.

### `Name is not available` errors
Global services (Storage, Service Bus, Web App, Function App) require unique names. Change `namePrefix` and/or `environment`.

### `InvalidTemplateDeployment` / Bicep validation failures
Run:
```bash
az deployment group validate \
  --resource-group <rg> \
  --template-file scripts/main.bicep \
  --parameters @scripts/parameters.json \
  --parameters namePrefix=<prefix> location=<location> environment=<env>
```

### App cannot authenticate users
Verify the exact values in app settings and Entra app registrations:
- `AZURE_AD_TENANT_ID`
- `AZURE_AD_CLIENT_ID`
- `AZURE_AD_API_AUDIENCE`
- `AZURE_AD_API_SCOPE`
- Redirect URIs

### Frontend cannot call backend API
Check:
- `FRONTEND_API_BASE_URL` points to `https://<function-app>.azurewebsites.net/api`
- API scope configured and consented in Entra
- Tokens contain expected `aud` and scope claims

---

## Files
- `scripts/main.bicep` — IaC template
- `scripts/parameters.json` — sample parameters
- `scripts/deploy.sh` — bootstrap/deploy script
- `scripts/setup-env.sh` — helper to print app/env settings
