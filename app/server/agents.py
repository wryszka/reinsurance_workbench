"""Agent narration with a cache-first wrapper. Caches LLM NARRATION ONLY (never structured outputs).

USE_CACHE wraps the latency of the FM-backed agent endpoints; structured decision/price/accumulation/capital
panels always call the UC functions live (see routes). Both heroes are pre-warmed by the reset job.
"""
import hashlib, json
from . import config, sql


def _key(endpoint: str, payload: dict) -> str:
    blob = json.dumps({"e": endpoint, "p": payload}, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:32]


def _ensure_cache():
    sql.query(f"""CREATE TABLE IF NOT EXISTS {config.CACHE_TABLE}
                  (cache_key STRING, endpoint STRING, response STRING, created_ts TIMESTAMP) USING DELTA""")


def _read(key: str):
    row = sql.query_one(f"SELECT response FROM {config.CACHE_TABLE} WHERE cache_key = '{key}' LIMIT 1")
    return row["response"] if row else None


def _write(key: str, endpoint: str, response: str):
    r = sql.esc(response)
    sql.query(f"""MERGE INTO {config.CACHE_TABLE} t USING (SELECT '{key}' k) s ON t.cache_key = s.k
                  WHEN NOT MATCHED THEN INSERT (cache_key, endpoint, response, created_ts)
                  VALUES ('{key}', '{sql.esc(endpoint)}', '{r}', current_timestamp())""")


def narrate(role_substr: str, question: str, data: dict, use_cache: bool = None) -> dict:
    """Call a narrate-only agent endpoint with structured data the caller already computed."""
    if use_cache is None:
        use_cache = config.USE_CACHE
    endpoint = config.resolve_endpoint(role_substr)
    payload = {"role": role_substr.replace("reinsurance-", ""), "question": question,
               "data_json": json.dumps(data, default=str)}
    key = _key(endpoint, payload)
    if use_cache:
        try:
            _ensure_cache()
            hit = _read(key)
            if hit is not None:
                return {"text": hit, "cache": "hit", "endpoint": endpoint}
        except Exception:
            pass
    try:
        w = config.get_workspace_client()
        resp = w.serving_endpoints.query(name=endpoint, dataframe_records=[payload])
        preds = resp.predictions if hasattr(resp, "predictions") else resp.as_dict().get("predictions")
        text = preds[0] if preds else ""
    except Exception as e:
        return {"text": f"[narration unavailable: {str(e)[:140]}]", "cache": "error", "endpoint": endpoint}
    if use_cache:
        try:
            _write(key, endpoint, text)
        except Exception:
            pass
    return {"text": text, "cache": ("miss" if use_cache else "bypass"), "endpoint": endpoint}
