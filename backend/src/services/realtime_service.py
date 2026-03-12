import json
import uuid

import azure.functions as func

from src.shared.signalr_utils import build_negotiate_payload


def negotiate_realtime(req: func.HttpRequest) -> func.HttpResponse:
    correlation_id = req.headers.get("x-correlation-id") or str(uuid.uuid4())
    pk = req.params.get("pk") or "demo-user"

    try:
        payload = build_negotiate_payload(user_id=pk)
    except Exception as exc:
        return func.HttpResponse(
            json.dumps(
                {
                    "error": "Failed to build SignalR negotiation payload.",
                    "details": str(exc),
                }
            ),
            status_code=500,
            mimetype="application/json",
            headers={"x-correlation-id": correlation_id},
        )

    return func.HttpResponse(
        json.dumps(payload),
        status_code=200,
        mimetype="application/json",
        headers={
            "x-correlation-id": correlation_id,
            "Cache-Control": "no-store",
        },
    )