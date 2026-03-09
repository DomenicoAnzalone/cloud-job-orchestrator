import os
from datetime import datetime, timedelta, timezone

from azure.core.exceptions import ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobSasPermissions, BlobServiceClient, generate_blob_sas


_blob_service_client = None
_blob_identity_service_client = None
_blob_identity_credential = None


def get_blob_service_client() -> BlobServiceClient:
    global _blob_service_client

    if _blob_service_client is not None:
        return _blob_service_client

    conn_str = os.environ["BLOB_CONNECTION"]
    _blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    return _blob_service_client


def get_blob_identity_service_client() -> BlobServiceClient:
    global _blob_identity_service_client, _blob_identity_credential

    if _blob_identity_service_client is not None:
        return _blob_identity_service_client

    account_url = os.environ["BLOB_ACCOUNT_URL"]
    _blob_identity_credential = DefaultAzureCredential()
    _blob_identity_service_client = BlobServiceClient(
        account_url=account_url,
        credential=_blob_identity_credential,
    )
    return _blob_identity_service_client


def _get_blob_account_name() -> str:
    account_url = os.environ["BLOB_ACCOUNT_URL"]
    return account_url.replace("https://", "").split(".", 1)[0]


def upload_text_output(pk: str, job_id: str, content: str) -> dict:
    service = get_blob_service_client()
    container_name = os.environ.get("BLOB_OUTPUT_CONTAINER", "output")
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


def generate_blob_read_sas_url(container_name: str, blob_name: str) -> dict:
    service = get_blob_identity_service_client()

    ttl_minutes = int(os.environ.get("OUTPUT_LINK_TTL_MINUTES", "10"))
    starts_on = datetime.now(timezone.utc) - timedelta(minutes=1)
    expires_on = starts_on + timedelta(minutes=ttl_minutes + 1)

    user_delegation_key = service.get_user_delegation_key(
        key_start_time=starts_on,
        key_expiry_time=expires_on,
    )

    sas_token = generate_blob_sas(
        account_name=_get_blob_account_name(),
        container_name=container_name,
        blob_name=blob_name,
        user_delegation_key=user_delegation_key,
        permission=BlobSasPermissions(read=True),
        start=starts_on,
        expiry=expires_on,
        protocol="https",
    )

    blob_client = service.get_blob_client(container=container_name, blob=blob_name)

    return {
        "url": f"{blob_client.url}?{sas_token}",
        "expiresAt": expires_on.isoformat(),
    }