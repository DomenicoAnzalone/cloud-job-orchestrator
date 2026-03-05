import json
import logging
import uuid
from datetime import datetime, timezone
import azure.functions as func
from src.shared.cosmos_utils import get_cosmos_container
from src.shared.servicebus_utils import enqueue_job

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def create_job(req: func.HttpRequest) -> func.HttpResponse:
    # GET: ancora stub
    if req.method == "GET":
        return func.HttpResponse("JobsApi stub (GET).", status_code=200)

    # POST /jobs
    correlation_id = req.headers.get("x-correlation-id") or str(uuid.uuid4())

    try:
        payload = req.get_json() or {}
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON body"}),
            status_code=400,
            mimetype="application/json",
        )

    pk = payload.get("pk") or "demo-user"
    job_type = payload.get("type") or "demo"

    job_id = str(uuid.uuid4())
    now = utc_now_iso()

    job_doc = {
        "id": job_id,
        "pk": pk,
        "type": job_type,
        "status": "queued",
        "progress": 0,
        "attempts": 0,
        "createdAt": now,
        "updatedAt": now,
        "correlationId": correlation_id,
        # salva parametri del job, (da vedere in futuro)
        "parameters": payload.get("parameters") or {},
    }

    logging.info("POST /jobs start jobId=%s pk=%s type=%s corr=%s", job_id, pk, job_type, correlation_id)

    # 1) write to Cosmos
    try:
        container = get_cosmos_container()
        container.create_item(body=job_doc)
    except Exception as e:
        logging.exception("Cosmos create_item failed jobId=%s corr=%s", job_id, correlation_id)
        return func.HttpResponse(
            json.dumps({"error": "Failed to create job in Cosmos", "details": str(e)}),
            status_code=500,
            mimetype="application/json",
        )

    # 2) enqueue in Service Bus (claim-check style: mando solo identificativi)
    sb_body = {"jobId": job_id, "pk": pk, "type": job_type, "correlationId": correlation_id}

    try:
        enqueue_job(sb_body, job_id=job_id, correlation_id=correlation_id)
    except Exception as e:
        logging.exception("Service Bus enqueue failed jobId=%s corr=%s", job_id, correlation_id)
        return func.HttpResponse(
            json.dumps({"error": "Failed to enqueue job to Service Bus", "details": str(e), "jobId": job_id}),
            status_code=500,
            mimetype="application/json",
        )

    # 3) 202 Accepted + statusUrl (endpoint /jobs/{id} verrà fatto dopo)
    status_url = f"{req.url.rstrip('/')}/{job_id}"

    return func.HttpResponse(
        json.dumps({"jobId": job_id, "statusUrl": status_url}),
        status_code=202,
        mimetype="application/json",
    )