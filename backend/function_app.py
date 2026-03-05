import logging
import azure.functions as func
from src.services.jobs_service import create_job

app = func.FunctionApp()

QUEUE_NAME = "q-jobs"

@app.function_name(name="JobsCreateApi")
@app.route(route="jobs", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def create_job_api(req: func.HttpRequest) -> func.HttpResponse: 
    return create_job(req)


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