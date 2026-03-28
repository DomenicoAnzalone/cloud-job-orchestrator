import logging
import azure.functions as func

from src.services.cleanup_service import cleanup_old_completed_jobs
from src.services.jobs_service import create_job, get_job_status, get_job_output_link
from src.services.worker_service import process_job_message
from src.services.realtime_service import negotiate_realtime

app = func.FunctionApp()

@app.function_name(name="JobsCreateApi")
@app.route(route="jobs", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def create_job_api(req: func.HttpRequest) -> func.HttpResponse: 
    return create_job(req)

@app.function_name(name="JobsGetStatusApi")
@app.route(route="jobs/{id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def jobs_get_status_api(req: func.HttpRequest) -> func.HttpResponse:
    return get_job_status(req)

@app.function_name(name="JobsGetOutputLinkApi")
@app.route(
    route="jobs/{id}/output-link",
    methods=["GET"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
def jobs_get_output_link_api(req: func.HttpRequest) -> func.HttpResponse:
    return get_job_output_link(req)

@app.function_name(name="JobsWorker")
@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name="q-jobs",
    connection="SERVICEBUS_CONNECTION",
)
def jobs_worker(msg: func.ServiceBusMessage) -> None:
    process_job_message(msg)

@app.function_name(name="RealtimeNegotiateApi")
@app.route(
    route="realtime/negotiate",
    methods=["GET", "POST"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
def realtime_negotiate_api(req: func.HttpRequest) -> func.HttpResponse:
    return negotiate_realtime(req)

@app.function_name(name="CleanupOldCompletedJobs")
@app.schedule(
    schedule="0 */15 * * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True,
)
def cleanup_old_completed_jobs_trigger(timer: func.TimerRequest) -> None:
    cleanup_old_completed_jobs(timer)