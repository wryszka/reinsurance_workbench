"""Thin SQL helper — runs statements on the warehouse via the app SP (SDK statement execution)."""
from . import config


def query(statement: str):
    """Return list[dict] rows. All values come back as strings from the API — cast in callers."""
    w = config.get_workspace_client()
    resp = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=config.WAREHOUSE_ID,
        catalog=config.CATALOG, schema=config.SCHEMA, wait_timeout="50s")
    result = resp.result
    if result is None or result.data_array is None:
        return []
    cols = [c.name for c in resp.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in result.data_array]


def query_one(statement: str):
    rows = query(statement)
    return rows[0] if rows else None


def esc(s: str) -> str:
    return (s or "").replace("'", "''")
