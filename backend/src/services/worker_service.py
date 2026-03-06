import json
import logging
import time
from datetime import datetime, timezone

import azure.functions as func
from azure.cosmos.exceptions import CosmosResourceNotFoundError

from src.shared.cosmos_utils import get_cosmos_container

NON_REPROCESSABLE_STATUSES = {"processing", "done", "failed", "canceled"}

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _replace_job(container, job_doc: dict) -> None:
    job_doc["updatedAt"] = utc_now_iso()
    container.replace_item(item=job_doc["id"], body=job_doc)


def process_job_message(msg: func.ServiceBusMessage) -> None:
    raw_body = msg.get_body().decode("utf-8", errors="replace")

    # Debug
    logging.info("JobsWorker received message body: %s", raw_body)

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        logging.exception("Invalid Service Bus message JSON.")
        raise

    job_id = payload.get("jobId")
    pk = payload.get("pk")
    correlation_id = payload.get("correlationId")

    if not job_id or not pk:
        logging.error(
            "Missing jobId/pk in Service Bus message. corr=%s body=%s",
            correlation_id,
            raw_body,
        )
        raise ValueError("Missing jobId/pk in Service Bus message")

    container = get_cosmos_container()

    try:
        job_doc = container.read_item(item=job_id, partition_key=pk)
    except CosmosResourceNotFoundError:
        logging.warning(
            "Job not found in Cosmos. jobId=%s pk=%s corr=%s",
            job_id,
            pk,
            correlation_id,
        )
        return

    current_status = (job_doc.get("status") or "").lower()

    # Idempotenza base: se è già terminale, non rielaborare
    if current_status in NON_REPROCESSABLE_STATUSES:
        logging.info(
            "Skipping already non-reprocessable job. jobId=%s status=%s corr=%s",
            job_id,
            current_status,
            correlation_id,
        )
        return

    # TO-DO Gestire la possibile race condition (se due worker superano simultaneamente il controllo sopra)
    # Un'idea è usare e-tag per rendere atomica la lettura + update dello status a "processing"
    try:
        # queued -> processing
        job_doc["status"] = "processing"
        job_doc["progress"] = 0.1
        job_doc["attempts"] = int(job_doc.get("attempts", 0)) + 1
        job_doc.pop("error", None)
        _replace_job(container, job_doc)

        # fake progress per demo
        for progress in (0.4, 0.7, 0.9):
            time.sleep(10)
            job_doc["status"] = "processing"
            job_doc["progress"] = progress
            _replace_job(container, job_doc)

        # processing -> done
        time.sleep(10)
        job_doc["status"] = "done"
        job_doc["progress"] = 1.0
        _replace_job(container, job_doc)

        logging.info(
            "Job completed successfully. jobId=%s pk=%s corr=%s",
            job_id,
            pk,
            correlation_id,
        )

    except Exception as exc:
        logging.exception(
            "Worker failed while processing jobId=%s pk=%s corr=%s",
            job_id,
            pk,
            correlation_id,
        )

        try:
            job_doc["status"] = "failed"
            job_doc["error"] = str(exc)
            _replace_job(container, job_doc)
        except Exception:
            logging.exception(
                "Failed to persist failed status for jobId=%s pk=%s corr=%s",
                job_id,
                pk,
                correlation_id,
            )

        raise