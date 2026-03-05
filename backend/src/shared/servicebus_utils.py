import os
import json
from typing import Any, Dict, Optional
from azure.servicebus import ServiceBusClient, ServiceBusMessage

QUEUE_NAME = "q-jobs"

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