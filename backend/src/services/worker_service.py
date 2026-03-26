import json
import logging
import time
from io import BytesIO
from datetime import datetime, timezone

import azure.functions as func
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from PIL import Image
from collections import deque
from statistics import median

from src.shared.blob_utils import read_blob_bytes, upload_output_file
from src.shared.cosmos_utils import get_cosmos_container
from src.shared.job_types import is_valid_job_type
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

def _is_forced_failure(job_doc: dict) -> bool:
    parameters = job_doc.get("parameters") or {}
    return bool(parameters.get("fail"))


def _build_error_payload(code: str, message: str, stage: str) -> dict:
    return {
        "code": code,
        "message": message,
        "stage": stage,
    }


def _remove_background(image_bytes: bytes) -> tuple[bytes, str, str]:
    with Image.open(BytesIO(image_bytes)) as image:
        rgba = image.convert("RGBA")
        width, height = rgba.size
        pixels = rgba.load()

        if width == 0 or height == 0:
            output_stream = BytesIO()
            rgba.save(output_stream, format="PNG")
            return output_stream.getvalue(), ".png", "image/png"

        # Stima del colore di sfondo usando campioni presi dai bordi.
        # Questo è più robusto di una soglia fissa su tutto l'immagine.
        border_samples: list[tuple[int, int, int]] = []

        step_x = max(1, width // 30)
        step_y = max(1, height // 30)

        for x in range(0, width, step_x):
            border_samples.append(pixels[x, 0][:3])
            border_samples.append(pixels[x, height - 1][:3])

        for y in range(0, height, step_y):
            border_samples.append(pixels[0, y][:3])
            border_samples.append(pixels[width - 1, y][:3])

        if not border_samples:
            border_samples = [pixels[0, 0][:3]]

        bg_r = int(median([c[0] for c in border_samples]))
        bg_g = int(median([c[1] for c in border_samples]))
        bg_b = int(median([c[2] for c in border_samples]))

        # Tolleranza: più alta = rimuove più sfondo, ma aumenta il rischio di toccare il soggetto.
        tolerance = 38
        tolerance_sq = tolerance * tolerance

        def is_background_pixel(x: int, y: int) -> bool:
            r, g, b, _ = pixels[x, y]
            dr = r - bg_r
            dg = g - bg_g
            db = b - bg_b
            return (dr * dr + dg * dg + db * db) <= tolerance_sq

        # Flood fill dai bordi: rimuove solo lo sfondo connesso ai bordi.
        visited = bytearray(width * height)
        remove = bytearray(width * height)
        queue = deque()

        def push(x: int, y: int) -> None:
            idx = y * width + x
            if not visited[idx]:
                visited[idx] = 1
                queue.append((x, y))

        for x in range(width):
            push(x, 0)
            push(x, height - 1)

        for y in range(height):
            push(0, y)
            push(width - 1, y)

        while queue:
            x, y = queue.popleft()
            idx = y * width + x

            if remove[idx]:
                continue

            if not is_background_pixel(x, y):
                continue

            remove[idx] = 1

            if x > 0:
                push(x - 1, y)
            if x + 1 < width:
                push(x + 1, y)
            if y > 0:
                push(x, y - 1)
            if y + 1 < height:
                push(x, y + 1)

        # Applica la trasparenza solo ai pixel identificati come sfondo.
        for y in range(height):
            row_base = y * width
            for x in range(width):
                if remove[row_base + x]:
                    r, g, b, _ = pixels[x, y]
                    pixels[x, y] = (r, g, b, 0)

        output_stream = BytesIO()
        rgba.save(output_stream, format="PNG")
        return output_stream.getvalue(), ".png", "image/png"


def _upscale_image(image_bytes: bytes, scale_factor: int = 2) -> tuple[bytes, str | None, str]:
    with Image.open(BytesIO(image_bytes)) as image:
        width, height = image.size
        resized = image.resize((width * scale_factor, height * scale_factor), Image.Resampling.LANCZOS)
        target_format = image.format or "PNG"

        output_stream = BytesIO()
        resized.save(output_stream, format=target_format)
        content_type = Image.MIME.get(target_format.upper(), "image/png")
        return output_stream.getvalue(), None, content_type


def _build_output_filename(input_blob_name: str, output_extension: str | None) -> str:
    original_name = input_blob_name.rsplit("/", 1)[-1] or "output.bin"

    if not output_extension:
        return original_name

    if "." in original_name:
        base = original_name.rsplit(".", 1)[0]
    else:
        base = original_name

    return f"{base}{output_extension}"


def _process_image(job_type: str, image_bytes: bytes) -> tuple[bytes, str | None, str]:
    if job_type == "background_removal":
        return _remove_background(image_bytes)
    if job_type == "image_upscale":
        return _upscale_image(image_bytes)

    raise ValueError(f"Unsupported job type: {job_type}")


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


        # Sleeping to allow the demo to display a “processing” status and progress before proceeding to the next stage.
        time.sleep(10)

        job_doc["status"] = "processing"
        job_doc["progress"] = 0.4

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

        input_ref = job_doc.get("inputRef") or {}
        input_container = input_ref.get("container")
        input_blob_name = input_ref.get("blobName")

        if not input_container or not input_blob_name:
            raise RuntimeError("Missing inputRef for job output build")

        image_bytes = read_blob_bytes(
            container_name=input_container,
            blob_name=input_blob_name,
        )
        job_type = job_doc.get("type")
        if not is_valid_job_type(job_type):
            raise RuntimeError(f"Invalid job type for processing: {job_type}")

        send_job_event(
            user_id=str(pk),
            event=build_log_event(job_id, f"Applying transformation: {job_type}"),
        )

        processed_bytes, output_extension, content_type = _process_image(job_type, image_bytes)
        output_file_name = _build_output_filename(input_blob_name, output_extension)

        # Sleeping to allow the demo to display a “processing” status and progress before proceeding to the next stage.
        time.sleep(10)

        job_doc["status"] = "processing"
        job_doc["progress"] = 0.7

        _replace_job(container, job_doc)
        _publish_job_update(job_doc, correlation_id)

        # =========================
        # UPLOAD OUTPUT
        # =========================
        stage = "upload-output"

        send_job_event(
            user_id=str(pk),
            event=build_log_event(job_id, "Uploading output to Blob Storage")
        )

        output_ref = upload_output_file(
            pk=pk,
            job_id=job_id,
            filename=output_file_name,
            content=processed_bytes,
            content_type=content_type,
        )

        # Sleeping to allow the demo to display a “processing” status and progress before proceeding to the next stage.
        time.sleep(10)

        job_doc["status"] = "processing"
        job_doc["progress"] = 0.7

        _replace_job(container, job_doc)
        _publish_job_update(job_doc, correlation_id)

        # =========================
        # FINALIZE
        # =========================
        stage = "finalize"

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
