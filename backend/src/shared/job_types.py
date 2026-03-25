ALLOWED_JOB_TYPES = {
    "background_removal",
    "image_upscale",
}


def is_valid_job_type(job_type: str | None) -> bool:
    return bool(job_type) and job_type in ALLOWED_JOB_TYPES
