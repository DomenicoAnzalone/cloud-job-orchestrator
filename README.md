# Cloud Job Orchestrator (CJO)
Queue-based orchestration for asynchronous file jobs (CSV files). The project follows a strict separation between **intake** (HTTP API) and **execution** (worker). Messages are kept small (claim-check style): the queue carries identifiers, while job state and metadata live in storage.

## Current milestone
Currently building the **Walking Skeleton**: a minimal end-to-end slice that proves the architecture and contracts before adding real processing logic and UI.

Target flow:
API (Azure Functions HTTP) → Cosmos DB (job state) → Service Bus (queue) → Worker (Azure Functions) → (later) Blob Storage outputs → (later) UI polling / notifications

## Repository structure
- `backend/` Azure Functions (Python): HTTP API + Worker
- `frontend/` Minimal demo UI (static page)
- `docs/` Architecture notes and contracts
- `samples/` Sample CSV and sample job payloads
- `infra/` Notes/scripts for Azure setup (no full IaC yet)

## Local development
This repo uses **Azure Functions Core Tools** for local runs.
Secrets/config are stored in `backend/local.settings.json` (not committed).
