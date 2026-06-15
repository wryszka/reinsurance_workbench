"""Config — all portability via env vars (set in app.yaml). No hardcoded catalog/schema/IDs."""
import os
from functools import lru_cache
from databricks.sdk import WorkspaceClient


def _flag(name, default=True):
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


CATALOG = os.getenv("CATALOG_NAME", "lr_dev_aws_us_catalog")
SCHEMA = os.getenv("SCHEMA_NAME", "bricksurance_re")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "a3b61648ea4809e3")
USE_CACHE = _flag("USE_CACHE", True)
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")
FM_ENDPOINT = os.getenv("FM_ENDPOINT", "databricks-claude-sonnet-4-6")

# Agent / model endpoints are resolved by substring at runtime (dev-prefix safe).
EP_SUPERVISOR_SUBSTR = os.getenv("EP_SUPERVISOR_SUBSTR", "reinsurance-supervisor")
EP_CHALLENGE_SUBSTR = os.getenv("EP_CHALLENGE_SUBSTR", "reinsurance-challenge")
EP_DATAQUALITY_SUBSTR = os.getenv("EP_DATAQUALITY_SUBSTR", "reinsurance-dataquality")
EP_EVENT_SUBSTR = os.getenv("EP_EVENT_SUBSTR", "reinsurance-event")
EP_PORTFOLIO_SUBSTR = os.getenv("EP_PORTFOLIO_SUBSTR", "reinsurance-portfolio")
EP_COUNTERPARTY_SUBSTR = os.getenv("EP_COUNTERPARTY_SUBSTR", "reinsurance-counterparty")

CACHE_TABLE = f"{CATALOG}.{SCHEMA}.cache_agent_responses"


def fqn(table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table}"


@lru_cache(maxsize=1)
def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


@lru_cache(maxsize=8)
def resolve_endpoint(substr: str) -> str:
    try:
        for e in get_workspace_client().serving_endpoints.list():
            if substr in e.name:
                return e.name
    except Exception:
        pass
    return substr


def workspace_host() -> str:
    h = os.getenv("DATABRICKS_HOST", "")
    if h:
        return h.rstrip("/")
    try:
        return get_workspace_client().config.host.rstrip("/")
    except Exception:
        return ""
