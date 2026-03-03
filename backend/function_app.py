import logging
import azure.functions as func

app = func.FunctionApp()

# HTTP API stub (WS1-03)
@app.function_name(name="JobsApi")
@app.route(route="jobs", methods=["GET", "POST"], auth_level=func.AuthLevel.ANONYMOUS)
def jobs_api(req: func.HttpRequest) -> func.HttpResponse:
    # Stub: no logic yet. Just prove the HTTP trigger is loaded and callable.
    if req.method == "POST":
        return func.HttpResponse("JobsApi stub (POST).", status_code=202)
    return func.HttpResponse("JobsApi stub (GET).", status_code=200)


# Service Bus worker stub (WS1-03)
@app.function_name(name="JobsWorker")
@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name="q-jobs",
    connection="SERVICEBUS_CONNECTION",
)
def jobs_worker(msg: func.ServiceBusMessage) -> None:
    # Stub: no processing yet. Just prove the SB trigger is loaded.
    body = msg.get_body().decode("utf-8", errors="replace")
    logging.info("JobsWorker received message body: %s", body)