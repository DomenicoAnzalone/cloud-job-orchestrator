# expected JSON schema:
# {
#   ....
#   "input": "/path/file.csv",
#   "output": "/path/outputFolder",
#   "parameters": {
#     "encoding": "Auto|UTF-8|Latin-1",
#     "columnNameNormalization": "lowercase|UPPERCASE|snake_case|none",
#     "stripSpecialCharsFromHeaders": "yes|no",
#     "dataTypeInference": "Auto|KeepAsString",
#     "whitespaceTrimming": "yes|no",
#     "emptyStringToNull": "yes|no",
#     "removeRowLengthMismatch": "yes|no",
#     "removeRowsWithNulls": {
#       "mode": "any|all|threshold|no",
#       "threshold": 1
#     },
#     "duplicateRowsRemoval": "yes|no"
#   }
# }

from __future__ import annotations

import csv
import io
import json
import os
import re
import datetime
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _resolve_path(value: Any, key: str = "path") -> str:
    """Accept str or dict with 'path'/'folder' key."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for k in (key, "path", "folder"):
            if k in value:
                return value[k]
    raise ValueError(f"Cannot resolve path from value: {value!r}")


def _is_yes(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("yes", "true", "1")
    return bool(value)


_PARAM_ALIASES: dict[str, str] = {
    "columnnormaliz": "columnNameNormalization",
    "columnnormalization": "columnNameNormalization",
    "column_name_normalization": "columnNameNormalization",
    "stripspecialcharsfromheaders": "stripSpecialCharsFromHeaders",
    "strip_special_chars": "stripSpecialCharsFromHeaders",
    "datatypeinference": "dataTypeInference",
    "data_type_inference": "dataTypeInference",
    "whitespacetrimming": "whitespaceTrimming",
    "whitespace_trimming": "whitespaceTrimming",
    "emptystringtonull": "emptyStringToNull",
    "empty_string_to_null": "emptyStringToNull",
    "removerowlengthmismatch": "removeRowLengthMismatch",
    "remove_row_length_mismatch": "removeRowLengthMismatch",
    "removerowswithnulls": "removeRowsWithNulls",
    "remove_rows_with_nulls": "removeRowsWithNulls",
    "duplicaterowsremoval": "duplicateRowsRemoval",
    "duplicate_rows_removal": "duplicateRowsRemoval",
    "encoding": "encoding",
}

_PARAM_DEFAULTS: dict[str, Any] = {
    "encoding": "Auto",
    "columnNameNormalization": "none",
    "stripSpecialCharsFromHeaders": "no",
    "dataTypeInference": "Auto",
    "whitespaceTrimming": "yes",
    "emptyStringToNull": "yes",
    "removeRowLengthMismatch": "no",
    "removeRowsWithNulls": {"mode": "no", "threshold": 1},
    "duplicateRowsRemoval": "no",
}


def _normalize_params(raw: dict) -> dict:
    normalized: dict = {}
    for k, v in raw.items():
        canonical = _PARAM_ALIASES.get(k.lower())
        if canonical:
            normalized[canonical] = v
        else:
            normalized[k] = v
    # fill defaults
    for k, default in _PARAM_DEFAULTS.items():
        if k not in normalized:
            normalized[k] = default
    # ensure removeRowsWithNulls is a dict
    rrn = normalized.get("removeRowsWithNulls", {})
    if isinstance(rrn, str):
        normalized["removeRowsWithNulls"] = {"mode": rrn, "threshold": 1}
    elif isinstance(rrn, dict):
        rrn.setdefault("mode", "no")
        rrn.setdefault("threshold", 1)
    return normalized


def _detect_encoding(path: str) -> str:
    with open(path, "rb") as f:
        sample = f.read(65536)
    try:
        sample.decode("utf-8-sig")
        # Check if it's actually utf-8-sig (BOM present)
        if sample.startswith(b"\xef\xbb\xbf"):
            return "utf-8-sig"
        return "utf-8"
    except UnicodeDecodeError:
        pass
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def _detect_delimiter(path: str, encoding: str) -> str:
    with open(path, encoding=encoding, errors="replace") as f:
        sample = f.read(8192)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except csv.Error:
        return ","


def _infer_column(series: pd.Series) -> pd.Series:
    """Attempt bool -> int -> float -> date -> string inference."""
    # Drop nulls for inspection
    non_null = series.dropna().astype(str).str.strip()
    if non_null.empty:
        return series

    # Bool
    bool_map = {"true": True, "false": False, "yes": True, "no": False, "1": True, "0": False}
    if non_null.str.lower().isin(bool_map.keys()).all():
        return series.map(lambda x: bool_map.get(str(x).strip().lower()) if pd.notna(x) else pd.NA)

    # Numeric
    try:
        converted = pd.to_numeric(non_null, errors="raise")
        # int or float?
        if (converted == converted.astype(int)).all():
            return pd.to_numeric(series, errors="coerce").astype("Int64")
        return pd.to_numeric(series, errors="coerce")
    except (ValueError, TypeError):
        pass

    # Date YYYY-MM-DD
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    if non_null.str.match(date_pattern).all():
        try:
            return pd.to_datetime(series, format="%Y-%m-%d", errors="coerce")
        except Exception:
            pass

    return series


def _normalize_col_name(name: str, mode: str, strip_special: bool) -> str:
    if mode == "lowercase":
        name = name.lower()
    elif mode == "UPPERCASE":
        name = name.upper()
    elif mode == "snake_case":
        name = re.sub(r"[\s\-]+", "_", name)
        name = re.sub(r"_+", "_", name)
        name = name.lower()
    # strip special chars after normalization
    if strip_special:
        name = re.sub(r"[^\w]", "", name)  # \w = letters, digits, underscore
    return name


def _update_job_file(job_path: str, job: dict) -> None:
    job["updatedAt"] = _now_utc()
    with open(job_path, "w", encoding="utf-8") as f:
        json.dump(job, f, indent=2, default=str)


def _set_progress(job: dict, job_path: str, progress: float) -> None:
    job["progress"] = round(progress, 2)
    _update_job_file(job_path, job)



# Main function
def run_csv_cleaning_job(job_json_path: str) -> dict:
    """
    Execute a CSV cleaning pipeline driven by a job JSON file.

    Reads the job JSON, runs the cleaning pipeline as configured, writes the
    cleaned CSV and a report JSON to the output folder, updates the job JSON
    in-place, and returns the updated job dict.
    """
    # Load job JSON
    with open(job_json_path, "r", encoding="utf-8") as f:
        job: dict = json.load(f)

    # Increment attempts
    job["attempts"] = job.get("attempts", 0) + 1
    job["status"] = "processing"
    job["error"] = None
    _update_job_file(job_json_path, job)

    start_time = _now_utc()

    try:
        # Validate & resolve paths
        if "input" not in job:
            raise ValueError("Job JSON missing required field: 'input'")
        if "output" not in job:
            raise ValueError("Job JSON missing required field: 'output'")
        if "parameters" not in job:
            raise ValueError("Job JSON missing required field: 'parameters'")

        input_path = _resolve_path(job["input"], "path")
        output_folder = _resolve_path(job["output"], "folder")
        raw_params = job["parameters"]

        if not isinstance(raw_params, dict):
            raise ValueError("'parameters' must be a JSON object")

        params = _normalize_params(raw_params)

        os.makedirs(output_folder, exist_ok=True)

        # Adding _cleaned suffix to input filename
        base_name = os.path.basename(input_path)
        stem, ext = os.path.splitext(base_name)
        if not ext:
            ext = ".csv"
        cleaned_filename = f"{stem}_cleaned{ext}"
        output_csv_path = os.path.join(output_folder, cleaned_filename)

        job_id = job.get("jobId") or job.get("id") or ""
        if job_id:
            report_filename = f"{stem}_cleaning_report_{job_id}.json"
        else:
            report_filename = f"{stem}_cleaning_report.json"
        report_path = os.path.join(output_folder, report_filename)

        report: dict = {
            "inputPath": input_path,
            "outputCsvPath": output_csv_path,
            "startTime": start_time,
            "endTime": None,
            "parametersUsed": params,
            "encodingUsed": None,
            "delimiterUsed": None,
            "rows_in": 0,
            "rows_out": 0,
            "removed_row_length_mismatch": 0,
            "removed_nulls": {"count": 0, "mode": params["removeRowsWithNulls"]["mode"]},
            "removed_duplicates": 0,
            "steps_applied": {},
        }

        _set_progress(job, job_json_path, 0.0)

        # Step 1: Encoding detection
        encoding_param = str(params.get("encoding", "Auto")).strip()
        if encoding_param.lower() == "auto":
            encoding_used = _detect_encoding(input_path)
        else:
            encoding_used = encoding_param

        report["encodingUsed"] = encoding_used
        report["steps_applied"]["encoding_detection"] = True
        _set_progress(job, job_json_path, 0.1)

        # Step 2: Delimiter detection
        delimiter_used = _detect_delimiter(input_path, encoding_used)
        report["delimiterUsed"] = delimiter_used
        report["steps_applied"]["delimiter_detection"] = True
        _set_progress(job, job_json_path, 0.2)

        # Step 3: Read CSV (with optional row-length mismatch removal)
        remove_mismatch = _is_yes(params.get("removeRowLengthMismatch", "no"))
        removed_mismatch_count = 0

        if remove_mismatch:
            with open(input_path, encoding=encoding_used, errors="replace", newline="") as f:
                reader = csv.reader(f, delimiter=delimiter_used)
                rows = list(reader)

            if not rows:
                raise ValueError("Input CSV is empty (no rows found).")

            header = rows[0]
            header_count = len(header)
            kept_rows = [header]
            for row in rows[1:]:
                if len(row) == header_count:
                    kept_rows.append(row)
                else:
                    removed_mismatch_count += 1

            buf = io.StringIO()
            writer = csv.writer(buf, delimiter=delimiter_used)
            writer.writerows(kept_rows)
            buf.seek(0)
            df = pd.read_csv(buf, delimiter=delimiter_used, dtype=str, keep_default_na=False)
        else:
            df = pd.read_csv(input_path, encoding=encoding_used, delimiter=delimiter_used,
                             dtype=str, keep_default_na=False)

        rows_in = len(df)
        report["rows_in"] = rows_in
        report["removed_row_length_mismatch"] = removed_mismatch_count
        report["steps_applied"]["removeRowLengthMismatch"] = remove_mismatch
        _set_progress(job, job_json_path, 0.3)

        # Step 4: Column name normalization
        col_norm_mode = str(params.get("columnNameNormalization", "none")).strip()
        strip_special = _is_yes(params.get("stripSpecialCharsFromHeaders", "no"))

        df.columns = [
            _normalize_col_name(c, col_norm_mode, strip_special)
            for c in df.columns
        ]
        report["steps_applied"]["columnNameNormalization"] = col_norm_mode != "none"

        # Step 5: Strip special chars from headers
        # Already handled inside _normalize_col_name when strip_special=True
        report["steps_applied"]["stripSpecialCharsFromHeaders"] = strip_special
        _set_progress(job, job_json_path, 0.4)

        # Step 6: Data type inference
        inference_mode = str(params.get("dataTypeInference", "Auto")).strip()
        if inference_mode.lower() == "auto":
            # Replace empty strings with NA before inference
            df = df.replace("", pd.NA)
            for col in df.columns:
                df[col] = _infer_column(df[col])
            report["steps_applied"]["dataTypeInference"] = True
        else:
            report["steps_applied"]["dataTypeInference"] = False
        _set_progress(job, job_json_path, 0.5)

        # Step 7: Whitespace trimming on string columns
        do_trim = _is_yes(params.get("whitespaceTrimming", "yes"))
        if do_trim:
            for col in df.select_dtypes(include="object").columns:
                df[col] = df[col].str.strip()
        report["steps_applied"]["whitespaceTrimming"] = do_trim
        _set_progress(job, job_json_path, 0.6)

        # Step 8: Empty string to null
        do_empty_null = _is_yes(params.get("emptyStringToNull", "yes"))
        if do_empty_null:
            df = df.replace("", pd.NA)
        report["steps_applied"]["emptyStringToNull"] = do_empty_null
        _set_progress(job, job_json_path, 0.7)

        # Step 9: Remove rows with nulls
        rrn = params["removeRowsWithNulls"]
        null_mode = str(rrn.get("mode", "no")).strip().lower()
        threshold = int(rrn.get("threshold", 1))
        before_null = len(df)

        if null_mode == "any":
            df = df.dropna(how="any")
        elif null_mode == "all":
            df = df.dropna(how="all")
        elif null_mode == "threshold":
            # keep rows with at least `threshold` non-null values
            df = df[df.notna().sum(axis=1) >= threshold]
        # else "no": do nothing

        removed_nulls = before_null - len(df)
        report["removed_nulls"] = {"count": removed_nulls, "mode": null_mode}
        report["steps_applied"]["removeRowsWithNulls"] = null_mode != "no"
        _set_progress(job, job_json_path, 0.8)

        # Step 10: Duplicate rows removal
        do_dedup = _is_yes(params.get("duplicateRowsRemoval", "no"))
        before_dedup = len(df)
        if do_dedup:
            df = df.drop_duplicates(keep="first")
        removed_dupes = before_dedup - len(df)
        report["removed_duplicates"] = removed_dupes
        report["steps_applied"]["duplicateRowsRemoval"] = do_dedup
        _set_progress(job, job_json_path, 0.9)

        # Write outputs
        df.to_csv(output_csv_path, index=False, encoding="utf-8")

        report["rows_out"] = len(df)
        report["endTime"] = _now_utc()

        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)

        job["status"] = "completed"
        job["progress"] = 1.0
        job["error"] = None
        _update_job_file(job_json_path, job)

    except Exception as exc:
        error_type = type(exc).__name__
        step = ""
        # Attempt to identify step from traceback context (best effort)
        job["status"] = "failed"
        job["error"] = {
            "message": str(exc),
            "type": error_type if error_type in ("ValueError", "IOError", "OSError") else "Exception",
            "step": step,
        }
        _update_job_file(job_json_path, job)
        raise

    return job