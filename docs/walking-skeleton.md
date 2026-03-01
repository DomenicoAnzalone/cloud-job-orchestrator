# WS1 — Walking Skeleton Spec

## Goal
Deliver a minimal end-to-end vertical slice using real Azure services with minimal logic:

POST job → Cosmos (queued) → Service Bus message → Worker (sleep) → Blob output → Cosmos (done) → GET status.

This validates:
- queue-based load leveling (decoupled intake vs processing)
- event-driven serverless execution (Service Bus-triggered worker)
- claim-check approach (message contains references/ids, payload lives in storage)

before implementing real CSV processing, auth, and real-time updates.

## Components (WS1)
- API: Azure Functions (HTTP trigger)
- Queue: Azure Service Bus queue `q-jobs`
- Worker: Azure Functions (Service Bus trigger)
- Metadata/state: Cosmos DB `cjo` / container `jobs` (partition key `/pk`)
- Storage: Blob containers `input`, `output`

## Workflow (WS1)
1. Client calls **POST /jobs**
2. API performs minimal validation and generates `jobId` (use `id == jobId`)
3. API writes a Job document in Cosmos DB with `status = queued`
4. API simulates saving the input CSV file to Blob Storage:
   - container: `input`
   - blob: `{pk}/{jobId}/source.csv`
5. API sends a message to Service Bus queue `q-jobs` containing `{ jobId, pk, type }`
6. Worker consumes the message and updates Cosmos to `processing`
7. Worker simulates work (`sleep` 10–20s) and updates `progress`
8. Worker simulates saving the output CSV file to Blob Storage:
   - container: `output`
   - blob: `{pk}/{jobId}/result.csv`
9. Worker updates Cosmos to `done` and stores `outputRef`
10. Client polls **GET /jobs/{jobId}** until completion
11. Client calls **GET /jobs/{jobId}/output-link** to obtain a SAS URL for the output

## API contract (WS1)
- POST `/jobs`
  - Response: `202 Accepted`
  - Body: `{ "jobId": "<jobId>", "status": "queued" }`
  - Optional header: `Location: /jobs/<jobId>`
- GET `/jobs/{jobId}`
  - Response: `200 OK` with the Job document (or `404 Not Found`)
- GET `/jobs/{jobId}/output-link`
  - `200 OK` with `{ "url": "<sasUrl>", "expiresAt": "<iso8601>" }` only when `status = done`
  - `409 Conflict` when job exists but is not `done`
  - `404 Not Found` when job does not exist

## Message contract (WS1)
Service Bus message (claim-check style, minimal payload):

```json
{ "jobId": "<jobId>", "pk": "<pk>", "type": "csv_cleaning" }
```

## Job states
Minimum:
- `queued`
- `processing`
- `done`
- `failed`
- `cancelRequested`
- `canceled`

Notes:
- Delivery is effectively “at-least-once”: the worker must be idempotent (safe to reprocess the same `jobId`).
- Suggested idempotency rule for WS1: if Cosmos status is already `done`/`failed`/`canceled`, worker no-ops and completes the message.

## What is fake in WS1
- Identity/auth: `pk` is simulated (fixed `"demo-user"`).
- Validation: minimal checks only.
- Input: no real user upload yet; the API writes a dummy file to Blob:
  - container `input`, blob `{pk}/{jobId}/source.csv`
- Processing: replaced by `sleep`.
- Output: fake file (later replaced by cleaned CSV + report).
- Notifications: no push/update stream; client uses polling.

## DB conventions (definitive names)

Blob containers:
- `input`
- `output`

Service Bus:
- Queue: `q-jobs`
- Dead-letter queue: enabled (built-in)
- `maxDeliveryCount = 5` (demo DLQ)

Cosmos DB (NoSQL / SQL API):
- Database: `cjo`
- Container: `jobs`
- Partition key: `/pk`
- WS1 simplification: backend always uses `pk = "demo-user"` for reads/updates.

## Job document example
```json
{
  "id": "<jobId>",
  "pk": "demo-user",
  "type": "csv_cleaning",
  "status": "queued",
  "progress": 0.0,
  "attempts": 0,
  "createdAt": "2026-02-25T10:00:00Z",
  "updatedAt": "2026-02-25T10:00:00Z",
  "inputRef": {
    "container": "input",
    "blob": "demo-user/<jobId>/source.csv"
  },
  "outputRef": {
    "container": "output",
    "blob": "demo-user/<jobId>/result.csv"
  },
  "error": null
}
```

## Out of scope (later phases)
- Real CSV upload from client + streaming
- Real CSV cleaning/validation/report generation
- Real identity/auth and per-user `pk`
- Real-time UI updates (SignalR/Event Grid/Change Feed)