import os
import sentry_sdk

def init_monitoring() -> None:
    """
    Initialize Sentry with performance tracking and environment context.
    """
    dsn = os.getenv("SENTRY_DSN")
    if not dsn:
        return

    sentry_sdk.init(
        dsn=dsn,
        environment=os.getenv("ENV", "development"),
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        traces_sample_rate=1.0,
        # Enable capturing log messages as breadcrumbs
        attach_stacktrace=True,
    )

def set_run_context(run_id: str) -> None:
    """
    Attach pipeline run ID to Sentry events.
    Also adds a breadcrumb to show when the run started.
    """
    sentry_sdk.set_tag("run_id", run_id)
    sentry_sdk.add_breadcrumb(
        category='pipeline',
        message=f'Starting pipeline execution for run_id: {run_id}',
        level='info',
    )