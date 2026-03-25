import json
import logging
import time
from datetime import datetime, timezone

import azure.functions as func
from azure.cosmos.exceptions import CosmosResourceNotFoundError

from src.shared.blob_utils import upload_text_output
from src.shared.cosmos_utils import get_cosmos_container
from src.shared.signalr_utils import (
    send_job_event,
    build_status_event,
    build_progress_event,
    build_completed_event,
    build_failed_event,
    build_log_event,
)

TERMINAL_SKIP_STATUSES = {"done", "canceled"}

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _replace_job(container, job_doc: dict) -> None:
    job_doc["updatedAt"] = utc_now_iso()
    container.replace_item(item=job_doc["id"], body=job_doc)

def _publish_job_update(job_doc: dict, correlation_id: str | None) -> None:

    # Legacy adapter → converte job_doc in eventi standard.
    try:
        user_id = str(job_doc.get("pk"))
        job_id = job_doc.get("id")
        status = job_doc.get("status")
        progress = job_doc.get("progress")
        error = job_doc.get("error")

        # STATUS event
        send_job_event(user_id, build_status_event(job_id, status))

        # PROGRESS event
        if progress is not None:
            send_job_event(user_id, build_progress_event(job_id, progress, status))

        # TERMINAL events
        if status == "done":
            send_job_event(user_id, build_completed_event(job_id))
        elif status == "failed":
            send_job_event(user_id, build_failed_event(job_id, error))

    except Exception:
        logging.exception(
            "SignalR publish failed for jobId=%s pk=%s corr=%s",
            job_doc.get("id"),
            job_doc.get("pk"),
            correlation_id,
        )

def _build_dummy_result(job_doc: dict) -> str:
    return (
        "Cloud Job Orchestrator - dummy worker output\n"
        f"jobId: {job_doc['id']}\n"
        f"pk: {job_doc.get('pk')}\n"
        f"type: {job_doc.get('type')}\n"
        f"status: done\n"
        f"attempts: {job_doc.get('attempts')}\n"
        f"generatedAt: {utc_now_iso()}\n"
    )


def _is_forced_failure(job_doc: dict) -> bool:
    parameters = job_doc.get("parameters") or {}
    return bool(parameters.get("fail"))


def _build_error_payload(code: str, message: str, stage: str) -> dict:
    return {
        "code": code,
        "message": message,
        "stage": stage,
    }


def process_job_message(msg: func.ServiceBusMessage) -> None:
    raw_body = msg.get_body().decode("utf-8", errors="replace")
    delivery_count = int(getattr(msg, "delivery_count", 0) or 0)

    logging.info(
        "JobsWorker received message. deliveryCount=%s body=%s",
        delivery_count,
        raw_body,
    )

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
    job_doc = None
    stage = "read-job"

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

    if current_status in TERMINAL_SKIP_STATUSES:
        logging.info(
            "Skipping terminal job. jobId=%s status=%s corr=%s",
            job_id,
            current_status,
            correlation_id,
        )
        return

    try:
        # =========================
        # START PROCESSING
        # =========================
        stage = "mark-processing"
        job_doc["status"] = "processing"
        job_doc["progress"] = 0.1
        job_doc["attempts"] = int(job_doc.get("attempts", 0)) + 1
        job_doc.pop("error", None)
        job_doc.pop("outputRef", None)

        send_job_event(
            user_id=str(pk),
            event=build_log_event(job_id, "Job started processing")
        )

        _replace_job(container, job_doc)
        _publish_job_update(job_doc, correlation_id)

        # =========================
        # FORCED FAILURE (DEMO)
        # =========================
        if _is_forced_failure(job_doc):
            stage = "forced-demo-failure"

            send_job_event(
                user_id=str(pk),
                event=build_log_event(job_id, "Forced failure triggered (demo mode)")
            )

            time.sleep(5)
            raise RuntimeError("Forced demo failure (parameters.fail=true)")

        # =========================
        # SIMULATED WORK
        # =========================
        stage = "simulate-work"

        for progress in (0.4, 0.7, 0.9):
            time.sleep(10)

            job_doc["status"] = "processing"
            job_doc["progress"] = progress

            _replace_job(container, job_doc)
            _publish_job_update(job_doc, correlation_id)

        # =========================
        # BUILD OUTPUT
        # =========================
        stage = "build-output"

        send_job_event(
            user_id=str(pk),
            event=build_log_event(job_id, "Building output result")
        )

        result_text = _build_dummy_result(job_doc)

        # =========================
        # UPLOAD OUTPUT
        # =========================
        stage = "upload-output"

        send_job_event(
            user_id=str(pk),
            event=build_log_event(job_id, "Uploading output to Blob Storage")
        )

        output_ref = upload_text_output(
            pk=pk,
            job_id=job_id,
            content=result_text,
        )

        # =========================
        # FINALIZE
        # =========================
        stage = "finalize"
        time.sleep(10)

        job_doc["status"] = "done"
        job_doc["progress"] = 1.0
        job_doc["outputRef"] = output_ref

        send_job_event(
            user_id=str(pk),
            event=build_log_event(job_id, "Job completed successfully")
        )

        _replace_job(container, job_doc)
        _publish_job_update(job_doc, correlation_id)

        logging.info(
            "Job completed successfully. jobId=%s pk=%s corr=%s outputRef=%s",
            job_id,
            pk,
            correlation_id,
            output_ref,
        )

    except Exception as exc:
        logging.exception(
            "Worker failed while processing jobId=%s pk=%s corr=%s stage=%s",
            job_id,
            pk,
            correlation_id,
            stage,
        )

        # LOG REALTIME ERROR
        send_job_event(
            user_id=str(pk),
            event=build_log_event(
                job_id,
                f"Job failed at stage '{stage}': {str(exc)}"
            )
        )

        if job_doc is not None:
            try:
                error_code = (
                    "DEMO_FORCED_FAILURE"
                    if _is_forced_failure(job_doc)
                    else "WORKER_EXECUTION_ERROR"
                )

                job_doc["status"] = "failed"
                job_doc["error"] = _build_error_payload(
                    code=error_code,
                    message=str(exc),
                    stage=stage,
                )

                _replace_job(container, job_doc)
                _publish_job_update(job_doc, correlation_id)

            except Exception:
                logging.exception(
                    "Failed to persist failed status for jobId=%s pk=%s corr=%s",
                    job_id,
                    pk,
                    correlation_id,
                )

        raise