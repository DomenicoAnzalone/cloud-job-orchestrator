import json
import logging
import os
import uuid
from datetime import datetime, timezone
import azure.functions as func
from azure.cosmos.exceptions import CosmosResourceNotFoundError, CosmosHttpResponseError

from src.shared.cosmos_utils import get_cosmos_container
from src.shared.servicebus_utils import enqueue_job
from src.shared.blob_utils import generate_blob_read_sas_url, upload_input_file
from src.shared.auth_utils import get_user_from_request
from src.shared.job_types import is_valid_job_type

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mark_job_failed(container, job_doc: dict, *, code: str, message: str, stage: str) -> None:
    job_doc["status"] = "failed"
    job_doc["error"] = {
        "code": code,
        "message": message,
        "stage": stage,
    }
    job_doc["updatedAt"] = utc_now_iso()
    container.replace_item(item=job_doc["id"], body=job_doc)

def _unauthorized_response(correlation_id: str) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"error": "Unauthorized"}),
        status_code=401,
        mimetype="application/json",
        headers={"x-correlation-id": correlation_id},
    )

def create_job(req: func.HttpRequest) -> func.HttpResponse:

    correlation_id = req.headers.get("x-correlation-id") or str(uuid.uuid4())

    logging.info("FORM KEYS: %s", list(req.form.keys()))
    logging.info("FILES KEYS: %s", list(req.files.keys()))

    # check authoritzation and get user by token
    try:
        user = get_user_from_request(req)
    except Exception:
        logging.warning("Unauthorized create job request. corr=%s", correlation_id)
        return _unauthorized_response(correlation_id)
    pk = user["userId"]
    job_type = req.form.get("type")
    image = req.files.get("image")
    fail_raw = req.form.get("fail")
    fail = str(fail_raw).lower() == "true"

    logging.info("POST /jobs received request pk=%s type=%s corr=%s", pk, job_type, correlation_id)
    if not is_valid_job_type(job_type):
        return func.HttpResponse(
            json.dumps({"error": "Invalid job type."}),
            status_code=400,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

    if image is None:
        return func.HttpResponse(
            json.dumps({"error": "Missing image file."}),
            status_code=400,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

    job_id = str(uuid.uuid4())
    now = utc_now_iso()
    image_name = os.path.basename(image.filename or "input.bin")
    image_bytes = image.stream.read()

    try:
        input_ref = upload_input_file(
            pk=pk,
            job_id=job_id,
            filename=image_name,
            content=image_bytes,
            content_type=image.content_type,
        )
    except Exception:
        logging.exception("Blob input upload failed jobId=%s corr=%s", job_id, correlation_id)
        return func.HttpResponse(
            json.dumps({"error": "Failed to upload input image"}),
            status_code=500,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

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
        "inputRef": input_ref,
        "parameters": {
            "fail": fail
        },
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
        try:
            _mark_job_failed(
                container,
                job_doc,
                code="ENQUEUE_FAILED",
                message="Failed to enqueue job to Service Bus.",
                stage="enqueue",
            )
        except Exception:
            logging.exception(
                "Failed to mark job as failed after enqueue error jobId=%s corr=%s",
                job_id,
                correlation_id,
            )
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
