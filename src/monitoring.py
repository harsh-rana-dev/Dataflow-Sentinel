import os
import sentry_sdk


def init_monitoring() -> None:
    """
    Initialize Sentry if DSN is provided.
    Safe for local development.
    """
    dsn = os.getenv("SENTRY_DSN")
    if not dsn:
        return

    sentry_sdk.init(
        dsn=dsn,
        environment=os.getenv("ENV", "development"),
    )


def set_run_context(run_id: str) -> None:
    """Attach pipeline run ID to Sentry events."""
    sentry_sdk.set_tag("run_id", run_id)

    