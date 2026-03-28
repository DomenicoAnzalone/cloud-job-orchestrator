import logging
from datetime import datetime, timedelta, timezone

from azure.core.exceptions import ResourceNotFoundError
from azure.cosmos.exceptions import CosmosResourceNotFoundError

from src.shared.cosmos_utils import get_cosmos_container
from src.shared.blob_utils import get_blob_service_client


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def delete_blob_reference(blob_service, ref: dict | None) -> None:
    if not ref:
        return

    container_name = ref.get("container")
    blob_name = ref.get("blobName")

    if not container_name or not blob_name:
        return

    try:
        blob_client = blob_service.get_blob_client(
            container=container_name,
            blob=blob_name,
        )
        blob_client.delete_blob(delete_snapshots="include")
        logging.info("Deleted blob %s/%s", container_name, blob_name)
    except ResourceNotFoundError:
        logging.info("Blob already missing %s/%s", container_name, blob_name)


def cleanup_old_completed_jobs(timer) -> None:
    cutoff = utc_now() - timedelta(hours=1)

    container = get_cosmos_container()
    blob_service = get_blob_service_client()

    query = """
    SELECT * FROM c
    WHERE c.status = @status
      AND IS_DEFINED(c.updatedAt)
    """

    params = [
        {"name": "@status", "value": "done"},
    ]

    candidates = list(
        container.query_items(
            query=query,
            parameters=params,
            enable_cross_partition_query=True,
        )
    )

    logging.info("Cleanup scan found %d done jobs", len(candidates))

    for doc in candidates:
        job_id = doc.get("id")
        pk = doc.get("pk")
        updated_at = doc.get("updatedAt")

        if not job_id or not pk or not updated_at:
            continue

        try:
            if parse_utc(updated_at) > cutoff:
                continue

            delete_blob_reference(blob_service, doc.get("inputRef"))
            delete_blob_reference(blob_service, doc.get("outputRef"))

            container.delete_item(item=job_id, partition_key=pk)
            logging.info("Deleted Cosmos job doc id=%s pk=%s", job_id, pk)

        except CosmosResourceNotFoundError:
            logging.info("Job already deleted id=%s pk=%s", job_id, pk)
        except Exception:
            logging.exception("Cleanup failed for id=%s pk=%s", job_id, pk)