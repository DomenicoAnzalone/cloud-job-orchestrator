import os

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

_blob_service_client = None


def get_blob_service_client() -> BlobServiceClient:
    global _blob_service_client

    if _blob_service_client is not None:
        return _blob_service_client

    conn_str = os.environ["BLOB_CONNECTION"]
    _blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    return _blob_service_client


def upload_text_output(pk: str, job_id: str, content: str) -> dict:
    service = get_blob_service_client()

    container_name = os.environ.get("BLOB_OUTPUT_CONTAINER", None)
    blob_name = f"{pk}/{job_id}/result.txt"

    container_client = service.get_container_client(container_name)

    try:
        container_client.create_container()
    except ResourceExistsError:
        pass

    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(content, overwrite=True)

    return {
        "container": container_name,
        "blobName": blob_name,
    }