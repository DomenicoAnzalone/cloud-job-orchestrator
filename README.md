# Cloud Job Orchestrator (CJO)

Walking skeleton: API (Azure Functions HTTP) → Cosmos DB → Service Bus → Worker (Azure Functions) → Blob Storage → Polling UI.

## Repository structure
- `backend/` Azure Functions (Python): HTTP API + Worker
- `frontend/` Minimal demo UI (static page)
- `docs/` Architecture notes and contracts
- `samples/` Sample CSV and sample job payloads
- `infra/` Notes/scripts for Azure setup (no full IaC yet)

## Local development (later)
This repo will use Azure Functions Core Tools to run locally.
Local secrets must go in `backend/local.settings.json` and MUST NOT be committed.