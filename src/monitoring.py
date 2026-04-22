import os
import sentry_sdk
import logging
from sentry_sdk.integrations.logging import LoggingIntegration


def init_monitoring() -> None:
    """
    Initialize Sentry with performance tracking and environment context.
    """
    dsn = os.getenv("SENTRY_DSN")

    # Optional: disable Sentry during tests
    if not dsn or os.getenv("ENV") == "TESTING":
        return

    sentry_logging = LoggingIntegration(
        level=logging.INFO,       
        event_level=logging.ERROR  
    )

    sentry_sdk.init(
        dsn=dsn,
        environment=os.getenv("ENV", "development"),
        integrations=[sentry_logging],
        traces_sample_rate=1.0,
        attach_stacktrace=True,
    )


def set_run_context(run_id: str) -> None:
    """
    Attach pipeline run ID to Sentry events.
    Also adds a breadcrumb to show when the run started.
    """
    sentry_sdk.set_tag("run_id", run_id)
    sentry_sdk.add_breadcrumb(
        category="pipeline",
        message=f"Starting pipeline execution for run_id: {run_id}",
        level="info",
    )