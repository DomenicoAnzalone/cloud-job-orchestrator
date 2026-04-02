# Infrastructure - Cloud Job Orchestrator

## 1. Panoramica
Questa cartella contiene il flusso di provisioning infrastrutturale Azure del progetto.

File rilevanti:
- `infra/deploy.sh`: crea/aggiorna Resource Group, valida, poi deploya l'infrastruttura.
- `infra/bicep/main.bicep`: definizione delle risorse Azure.
- `infra/bicep/parameters.json` e `infra/bicep/parameters.test.json`: valori parametro di esempio (ovviamente da sostituire con quelli reali).
- `infra/setup-env.sh`: legge gli app settings da Function App/Web App e stampa variabili utili da inserire al posto dei placehlder in `.env` e `backend/local.settings.json`.

## 2. Prerequisiti
Permessi Azure richiesti:
- Permesso di creare risorse nel subscription/resource group target.
- Permesso di creare role assignment (il template crea assegnazioni RBAC).

Tool richiesti (obbligatori perché usati dagli script):
- `bash`
- `az` (Azure CLI)
- `jq`

Tool aggiuntivi necessari per deploy completo applicativo:
- `zip` (per creare pacchetti deployabili, principalmnte per il frontend)
- `python3` + `pip` (backend Azure Functions)
- `node` + `npm` (frontend Node/Express)

## 3. Installazione tool richiesti
Installare i tool in base al proprio OS. Verificare che siano disponibili nel PATH:

```bash
az version
jq --version
zip -v
python3 --version
npm --version
```

## 4. Autenticazione Azure
Eseguire login e selezionare la subscription:

```bash
az login
az account set --subscription "<subscription-id-o-name>"
az account show --query id -o tsv
```

## 5. Configurazione progetto

### 5.1 File parametri infrastruttura
Creare un file parametri dedicato ambiente:

```bash
cp infra/bicep/parameters.json infra/bicep/parameters.<env>.json
```

Aggiornare nel file almeno:
- `environment`
- `location`
- `namePrefix`
- `azureAdTenantId`
- `azureAdClientId`
- `azureAdApiAudience`
- `azureAdApiScope`

I placeholder `<set-manually>` presenti nei parametri non sono validi in produzione: ovviamente vanno sostituiti.

### 5.2 Config locale backend
Creare file locale Functions:

```bash
cp backend/local.settings.example.json backend/local.settings.json
```

### 5.3 Config locale frontend
Creare variabili ambiente locali:

```bash
cp .env.example .env
```

## 6. Riferimento variabili ambiente
Le variabili seguenti sono usate dal codice backend/frontend o impostate da Bicep negli app settings.

### Runtime/backend
- `FUNCTIONS_WORKER_RUNTIME`: runtime Functions (python).
- `AzureWebJobsStorage`: storage usato dal runtime Functions.
- `DEPLOYMENT_STORAGE_CONNECTION_STRING`: storage aggiuntivo per operazioni di deploy/artifacts.
- `SERVICEBUS_CONNECTION`: connessione Service Bus usata da trigger e producer.
- `SERVICEBUS_JOBS_QUEUE`: coda job (default `q-jobs`).
- `COSMOS_ENDPOINT`: endpoint account Cosmos DB.
- `COSMOS_KEY`: key account Cosmos DB.
- `COSMOS_DB`: database Cosmos (default `cjo`).
- `COSMOS_CONTAINER`: container Cosmos (default `jobs`).
- `BLOB_CONNECTION`: connessione Blob Storage.
- `BLOB_ACCOUNT_URL`: URL account blob (usato per SAS/output link).
- `BLOB_INPUT_CONTAINER`: container input (default `input`).
- `BLOB_OUTPUT_CONTAINER`: container output (default `output`).
- `OUTPUT_LINK_TTL_MINUTES`: TTL link download output.
- `SIGNALR_CONNECTION_STRING`: connessione Azure SignalR.
- `SIGNALR_HUB_NAME`: hub SignalR (default `jobs`).
- `WORKER_DELAY_SECONDS`: ritardo artificiale worker (demo/testing).

### Auth (Entra ID / Azure AD)
- `AZURE_AD_TENANT_ID`: tenant ID.
- `AZURE_AD_CLIENT_ID`: client ID app frontend usata da MSAL.
- `AZURE_AD_API_AUDIENCE`: audience API backend (`api://...`).
- `AZURE_AD_API_SCOPE`: scope OAuth richiesto dal frontend.
- `AZURE_AD_AUTHORITY`: authority tenant login.

### Frontend
- `FRONTEND_API_BASE_URL`: URL base API.
- `API_BASE_URL`: alias compatibilità API base URL.
- `CJO_API_BASE_URL`: alias compatibilità API base URL.
- `PORT`: porta server Express frontend.

## 7. Deployment

### 7.1 Validazione / what-if (supportato)
Lo script `infra/deploy.sh` esegue sempre `validate` prima del deploy.

Esempio:

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

### 7.2 Deploy reale infrastruttura
Esempio senza what-if:

```bash
./infra/deploy.sh \
  --resource-group rg-cjo-dev \
  --location eastus \
  --prefix cjo \
  --environment dev \
  --params infra/bicep/parameters.dev.json
```

Lo script crea/aggiorna il Resource Group, poi deploya `infra/bicep/main.bicep`.

### 7.3 Deploy applicazione backend (Function App)
Dopo il provisioning va pubblicato anche il codice backend (il template crea solo infrastruttura + app settings).

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

### 7.4 Deploy applicazione frontend (Web App)
Dopo il provisioning va pubblicato anche il codice frontend.

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

## 8. Spiegazione parametri `deploy.sh`
Parametri supportati:
- `-g, --resource-group` (obbligatorio): nome Resource Group target.
- `-l, --location` (obbligatorio): region Azure (es. `eastus`).
- `-p, --prefix` (obbligatorio): prefisso naming, regex `^[a-z0-9]{3,12}$`.
- `-e, --environment` (opzionale, default `dev`): label ambiente, regex `^[a-z0-9-]{2,15}$`.
- `--subscription` (opzionale): subscription ID o nome.
- `--params` (opzionale): file parametri Bicep (default `infra/bicep/parameters.json`).
- `--what-if` (opzionale): esegue ARM what-if prima del create.
- `-h, --help`: help.

## 9. Output attesi
A fine deploy `infra/deploy.sh` stampa:
- Function App name
- Function API URL (`.../api`)
- Web App name
- Web App URL
- Storage Account name
- Service Bus Namespace name
- Cosmos DB Account name
- SignalR Service name

Questi output servono per i deploy applicativi (`--name`) e per la verifica finale.

## 10. Verifica

### 10.1 Verifica stato app Azure
```bash
az functionapp show -g rg-cjo-dev -n <function-app-name-output> --query "state" -o tsv
az webapp show -g rg-cjo-dev -n <web-app-name-output> --query "state" -o tsv
```

### 10.2 Verifica API backend
```bash
curl -i "https://<function-app-name-output>.azurewebsites.net/api/jobs/non-existent-id"
```

Risultato atteso (se tutto è andato bene): risposta HTTP dall'endpoint (tipicamente `404` se job non esiste).

### 10.3 Verifica frontend
Aprire in browser:

```text
https://<web-app-name-output>.azurewebsites.net
```

La web app deve rispondere e servire la UI.

### 10.4 Estrazione variabili reali dagli app settings
```bash
./infra/setup-env.sh \
  --resource-group rg-cjo-dev \
  --function-app <function-app-name-output> \
  --web-app <web-app-name-output>
```

Lo script stampa valori da copiare in `.env` e `backend/local.settings.json`.
