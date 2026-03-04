import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import azure.functions as func
from azure.cosmos import CosmosClient
from azure.servicebus import ServiceBusClient, ServiceBusMessage

app = func.FunctionApp()

QUEUE_NAME = "q-jobs"

# Cache "soft" del container Cosmos (riusato tra invocazioni nello stesso worker process)
_cosmos_container = None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_cosmos_container():
    global _cosmos_container
    if _cosmos_container is not None:
        return _cosmos_container

    endpoint = os.environ["COSMOS_ENDPOINT"]
    key = os.environ["COSMOS_KEY"]
    db_name = os.environ.get("COSMOS_DB", "cjo")
    container_name = os.environ.get("COSMOS_CONTAINER", "jobs")

    client = CosmosClient(endpoint, credential=key)
    db = client.get_database_client(db_name)
    _cosmos_container = db.get_container_client(container_name)
    return _cosmos_container


def enqueue_job(message_body: Dict[str, Any], job_id: str, correlation_id: str) -> None:
    conn_str = os.environ["SERVICEBUS_CONNECTION"]

    sb_client = ServiceBusClient.from_connection_string(conn_str)
    with sb_client:
        sender = sb_client.get_queue_sender(queue_name=QUEUE_NAME)
        with sender:
            msg = ServiceBusMessage(
                json.dumps(message_body),
                content_type="application/json",
                message_id=job_id,          # utile ad abilitare deduplicazione
                correlation_id=correlation_id
            )
            sender.send_messages(msg)


@app.function_name(name="JobsApi")
@app.route(route="jobs", methods=["GET", "POST"], auth_level=func.AuthLevel.ANONYMOUS)
def jobs_api(req: func.HttpRequest) -> func.HttpResponse:
    # GET: ancora stub (WS1-05/WS1-06 lo completeranno)
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

@app.function_name(name="JobsWorker")
@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name=QUEUE_NAME,
    connection="SERVICEBUS_CONNECTION",
)
def jobs_worker(msg: func.ServiceBusMessage) -> None:
    # Stub: no processing yet. Just prove the SB trigger is loaded.
    body = msg.get_body().decode("utf-8", errors="replace")
    logging.info("JobsWorker received message body: %s", body)