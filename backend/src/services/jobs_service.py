import json
import logging
import uuid
from datetime import datetime, timezone
import azure.functions as func
from azure.cosmos.exceptions import CosmosResourceNotFoundError, CosmosHttpResponseError

from src.shared.cosmos_utils import get_cosmos_container
from src.shared.servicebus_utils import enqueue_job
from src.shared.blob_utils import generate_blob_read_sas_url
from src.shared.auth_utils import get_user_from_request

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _unauthorized_response(correlation_id: str) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"error": "Unauthorized"}),
        status_code=401,
        mimetype="application/json",
        headers={"x-correlation-id": correlation_id},
    )

def create_job(req: func.HttpRequest) -> func.HttpResponse:

    correlation_id = req.headers.get("x-correlation-id") or str(uuid.uuid4())

    try:
        payload = req.get_json() or {}
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON body"}),
            status_code=400,
            mimetype="application/json",
        )

    # TO-DO: validare payload

    # check authoritzation and get user by token
    try:
        user = get_user_from_request(req)
    except Exception:
        logging.warning("Unauthorized create job request. corr=%s", correlation_id)
        return _unauthorized_response(correlation_id)
    pk = user["userId"]
    job_type = payload.get("type") or "demo"

    # TO-DO: validare job_type

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
    except Exception:
        logging.exception("Cosmos create_item failed jobId=%s corr=%s", job_id, correlation_id)
        return func.HttpResponse(
            json.dumps({"error": "Failed to create job in Cosmos"}),
            status_code=500,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

    # 2) enqueue in Service Bus (claim-check style: mando solo identificativi)
    sb_body = {"jobId": job_id, "pk": pk, "type": job_type, "correlationId": correlation_id}

    try:
        enqueue_job(sb_body, job_id=job_id, correlation_id=correlation_id)
    except Exception:
        logging.exception("Service Bus enqueue failed jobId=%s corr=%s", job_id, correlation_id)
        return func.HttpResponse(
            json.dumps({"error": "Failed to enqueue job to Service Bus", "jobId": job_id}),
            status_code=500,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

    # 3) 202 Accepted + statusUrl (endpoint /jobs/{id} verrà fatto dopo)
    status_url = f"{req.url.rstrip('/')}/{job_id}"

    return func.HttpResponse(
        json.dumps({"jobId": job_id, "statusUrl": status_url}),
        status_code=202,
        mimetype="application/json",
        headers={"x-correlation-id": correlation_id},
    )

def get_job_status(req: func.HttpRequest) -> func.HttpResponse:
    correlation_id = req.headers.get("x-correlation-id") or str(uuid.uuid4())

    # Route: /jobs/{id}  -> id sta nei route_params
    job_id = None
    try:
        job_id = req.route_params.get("id")
    except Exception:
        job_id = None

    if not job_id:
        return func.HttpResponse(
            json.dumps({"error": "Missing job id in route (/jobs/{id})."}),
            status_code=400,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

    # check authoritzation and get user by token
    try:
        user = get_user_from_request(req)
    except Exception:
        logging.warning("Unauthorized get status request. jobId=%s corr=%s", job_id, correlation_id)
        return _unauthorized_response(correlation_id)
    pk = user["userId"]

    # Check formato corretto
    try:
        uuid.UUID(job_id)
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid job id format (expected UUID)."}),
            status_code=400,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

    container = get_cosmos_container()

    try:
        doc = container.read_item(item=job_id, partition_key=pk)
    
    # TO-DO: gestire eccezione se job non trovato (404)
    except CosmosResourceNotFoundError:
        return func.HttpResponse(
            json.dumps({"error": "Job not found."}),
            status_code=404,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )
    except CosmosHttpResponseError:
        logging.exception("Cosmos DB error while reading job status.")
        return func.HttpResponse(
            json.dumps({"error": "Cosmos DB error."}),
            status_code=500,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )
    except Exception:
        logging.exception("Unexpected error while reading job status.")
        return func.HttpResponse(
            json.dumps({"error": "Unexpected server error."}),
            status_code=500,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

    payload = {
        "status": doc.get("status") or "queued",
        "progress": doc.get("progress", 0),
        "attempts": doc.get("attempts", 0),
        "outputRef": doc.get("outputRef"),
        "error": doc.get("error"),
        "updatedAt": doc.get("updatedAt"),
    }

    return func.HttpResponse(
        json.dumps(payload),
        status_code=200,
        mimetype="application/json",
        headers={"x-correlation-id": correlation_id},
    )

def get_job_output_link(req: func.HttpRequest) -> func.HttpResponse:
    correlation_id = req.headers.get("x-correlation-id") or str(uuid.uuid4())

    try:
        job_id = req.route_params.get("id")
    except Exception:
        job_id = None

    if not job_id:
        return func.HttpResponse(
            json.dumps({"error": "Missing job id in route (/jobs/{id}/output-link)."}),
            status_code=400,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

    # check authoritzation and get user by token
    try:
        user = get_user_from_request(req)
    except Exception:
        logging.warning("Unauthorized get output link request. jobId=%s corr=%s", job_id, correlation_id)
        return _unauthorized_response(correlation_id)
    pk = user["userId"]

    try:
        uuid.UUID(job_id)
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid job id format (expected UUID)."}),
            status_code=400,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

    container = get_cosmos_container()

    try:
        doc = container.read_item(item=job_id, partition_key=pk)
    except CosmosResourceNotFoundError:
        return func.HttpResponse(
            json.dumps({"error": "Job not found."}),
            status_code=404,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )
    except CosmosHttpResponseError:
        logging.exception("Cosmos DB error while reading job output link.")
        return func.HttpResponse(
            json.dumps({"error": "Cosmos DB error."}),
            status_code=500,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )
    except Exception:
        logging.exception("Unexpected error while reading job output link.")
        return func.HttpResponse(
            json.dumps({"error": "Unexpected server error."}),
            status_code=500,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

    status = (doc.get("status") or "").lower()
    output_ref = doc.get("outputRef") or {}
    output_container = output_ref.get("container")
    blob_name = output_ref.get("blobName")

    if status != "done":
        return func.HttpResponse(
            json.dumps(
                {
                    "error": "Output not available yet.",
                    "status": status or "unknown",
                }
            ),
            status_code=409,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

    if not output_container or not blob_name:
        logging.error(
            "Job is done but outputRef is missing/incomplete. jobId=%s pk=%s corr=%s",
            job_id,
            pk,
            correlation_id,
        )
        return func.HttpResponse(
            json.dumps({"error": "Job output metadata missing."}),
            status_code=500,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

    try:
        sas_payload = generate_blob_read_sas_url(
            container_name=output_container,
            blob_name=blob_name,
        )
    except Exception:
        logging.exception(
            "Failed to generate SAS output link. jobId=%s pk=%s corr=%s",
            job_id,
            pk,
            correlation_id,
        )
        return func.HttpResponse(
            json.dumps({"error": "Failed to generate output link."}),
            status_code=500,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

    return func.HttpResponse(
        json.dumps(
            {
                "downloadUrl": sas_payload["url"],
                "expiresAt": sas_payload["expiresAt"],
            }
        ),
        status_code=200,
        mimetype="application/json",
        headers={
            "x-correlation-id": correlation_id,
            "Cache-Control": "no-store",
        },
    )
