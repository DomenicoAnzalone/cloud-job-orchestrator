# WS1 — Walking Skeleton Spec 

## Goal
Deliver a minimal end-to-end vertical slice using real Azure services with minimal logic:
POST job → Cosmos (queued) → Service Bus message → Worker (sleep) → Blob output → Cosmos (done) → GET status.

This validates service integration and the claim-check pattern before implementing real CSV processing, auth, and real-time updates.

## Workflow (WS1)
1. Client calls **POST /jobs**
2. API performs minimal validation and generates `jobId`
3. API writes a Job document in Cosmos DB with `status = queued`
4. API simulates saving the input CSV file to Blob Storage (container **input**) using the convention:
   - `input/{pk}/{jobId}/source.csv`
5. API sends a message to Service Bus queue **q-jobs** containing `{ jobId, pk, type }`
6. Worker consumes the message and updates Cosmos to `processing`
7. Worker simulates work (`sleep` 10–20s) and updates `progress`
8. Worker simulates saving the output CSV file to Blob Storage (container **output**) using the convention:
   - `output/{pk}/{jobId}/source.csv` (probably the same input file)
9. Worker updates Cosmos to `done` and stores `outputRef`
10. Client polls **GET /jobs/{jobId}** until completion
11. Client calls **GET /jobs/{jobId}/output-link** to obtain a SAS URL for the output

## Job states
Minimum:
- `queued`
- `processing`
- `done`
- `failed`
- `cancelRequested`
- `canceled`

## What is fake in WS1
- Identity/auth: `pk` is simulated (e.g., fixed `"demo-user"`).
- Validation: minimal checks only.
- Input: no real user upload yet; the API simulates an input CSV file by writing a dummy file to Blob `input/{pk}/{jobId}/source.csv`.
- Processing: replaced by `sleep`.
- Output: same input CSV file (later replaced by cleaned CSV + report).

## Blob conventions
Containers already created:
- `input`
- `output`

### Cosmos Job document (minimal)
Partition key: `/pk`

Json Example:
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
    "blob": "input/demo-user/<jobId>/source.csv"
  },
  "outputRef": null,
  "error": null
}