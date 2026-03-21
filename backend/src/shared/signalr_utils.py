import base64
import hashlib
import hmac
import json
import os
import time
import urllib.parse
import urllib.request
from typing import Any


def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")


def _parse_signalr_connection_string() -> tuple[str, str]:
    conn = os.environ["SIGNALR_CONNECTION_STRING"]

    parts = {}
    for item in conn.split(";"):
        if "=" in item:
            key, value = item.split("=", 1)
            parts[key] = value

    endpoint = parts.get("Endpoint")
    access_key = parts.get("AccessKey")

    if not endpoint or not access_key:
        raise ValueError("Invalid SIGNALR_CONNECTION_STRING.")

    return endpoint.rstrip("/"), access_key


def _build_jwt(audience: str, signing_key: str, expires_in_seconds: int, extra_claims: dict[str, Any] | None = None) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "aud": audience,
        "exp": int(time.time()) + expires_in_seconds,
    }

    if extra_claims:
        payload.update(extra_claims)

    encoded_header = _base64url_encode(
        json.dumps(header, separators=(",", ":")).encode("utf-8")
    )
    encoded_payload = _base64url_encode(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    )

    signing_input = f"{encoded_header}.{encoded_payload}".encode("utf-8")
    signature = hmac.new(
        signing_key.encode("utf-8"),
        signing_input,
        hashlib.sha256,
    ).digest()

    encoded_signature = _base64url_encode(signature)
    return f"{encoded_header}.{encoded_payload}.{encoded_signature}"


def build_negotiate_payload(user_id: str) -> dict[str, str]:
    endpoint, access_key = _parse_signalr_connection_string()
    hub_name = os.environ.get("SIGNALR_HUB_NAME", "jobs")
    url = f"{endpoint}/client/?hub={urllib.parse.quote(hub_name)}"

    token = _build_jwt(
        audience=url,
        signing_key=access_key,
        expires_in_seconds=1800,
        extra_claims={"nameid": user_id},
    )

    return {
        "url": url,
        "accessToken": token,
    }


def send_signalr_message_to_user(user_id: str, target: str, arguments: list[Any]) -> None:
    endpoint, access_key = _parse_signalr_connection_string()
    hub_name = os.environ.get("SIGNALR_HUB_NAME", "jobs")

    user_id_encoded = urllib.parse.quote(user_id, safe="")
    hub_name_encoded = urllib.parse.quote(hub_name, safe="")
    request_url = f"{endpoint}/api/v1/hubs/{hub_name_encoded}/users/{user_id_encoded}"

    jwt_token = _build_jwt(
        audience=request_url,
        signing_key=access_key,
        expires_in_seconds=300,
    )

    body = json.dumps(
        {
            "target": target,
            "arguments": arguments,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        url=request_url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {jwt_token}",
        },
    )

    with urllib.request.urlopen(req) as response:
        if response.status >= 300:
            raise RuntimeError(f"SignalR publish failed with HTTP {response.status}")

def send_job_event(user_id: str, event: dict) -> None:
    
    # Wrapper standard per eventi job → forza target e schema unico.
    send_signalr_message_to_user(
        user_id=user_id,
        target="jobUpdated",
        arguments=[event],
    )

def build_status_event(job_id: str, status: str) -> dict:
    return {
        "jobId": job_id,
        "type": "status",
        "status": status,
    }


def build_progress_event(job_id: str, progress: float, status: str = "processing") -> dict:
    return {
        "jobId": job_id,
        "type": "progress",
        "status": status,
        "progress": progress,
    }


def build_completed_event(job_id: str, download_url: str | None = None) -> dict:
    return {
        "jobId": job_id,
        "type": "completed",
        "status": "done",
        "downloadUrl": download_url,
    }


def build_failed_event(job_id: str, error: dict) -> dict:
    return {
        "jobId": job_id,
        "type": "failed",
        "status": "failed",
        "error": error,
    }


def build_log_event(job_id: str, message: str) -> dict:
    return {
        "jobId": job_id,
        "type": "log",
        "message": message,
    }