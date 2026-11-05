"""Job-run identifiers for the audit and DQ logs (job_id, run_id, task_run_id, task_key).

On serverless compute the Spark conf keys that audit_log and dq_gate_log used to read
(spark.databricks.job.*) are not readable, so those columns came back null. The job
passes the values in as task parameters instead (resources/jobs/*.yml, base_parameters),
and each notebook calls set_from_widgets() once at the top. The loggers use these values
first and fall back to the Spark conf, so classic compute still works unchanged.
"""

_CONTEXT = {}


def set_from_widgets(dbutils) -> None:
    """Read the four job-context widgets. A widget that is not set becomes None."""
    for name in ("job_id", "job_run_id", "task_run_id", "task_key"):
        try:
            _CONTEXT[name] = dbutils.widgets.get(name).strip() or None
        except Exception:
            _CONTEXT[name] = None


def job_value(spark, conf_key: str, name: str):
    """The job-context value for `name`, else the Spark conf `conf_key`, else None."""
    if _CONTEXT.get(name):
        return _CONTEXT[name]
    try:
        return spark.conf.get(conf_key, None)
    except Exception:
        # Serverless raises CONFIG_NOT_AVAILABLE for these keys instead of returning None
        return None
