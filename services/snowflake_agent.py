"""
Snowflake Cortex Agent client.
Uses Cortex Agents REST API when available (streaming-capable); falls back to
DATA_AGENT_RUN via SQL connector.
"""
import json
import logging
import os

import requests

from services.snowflake_service import get_connection

logger = logging.getLogger(__name__)

AGENT_NAME = "SNOWFLAKE"
USE_REST = os.getenv("AGENT_USE_REST_API", "false").lower() == "true"
USE_STREAM = os.getenv("AGENT_STREAM", "true").lower() == "true"


def _msg_to_api_format(msg: dict) -> dict:
    """Convert stored message to Cortex API message format."""
    role = msg.get("role", "user")
    content = msg.get("content", [])
    if role == "user":
        text = content if isinstance(content, str) else ""
        return {"role": "user", "content": [{"type": "text", "text": text}]}
    # assistant: content is list of content items; pass through if already structured
    if isinstance(content, list) and content and isinstance(content[0], dict):
        return {"role": "assistant", "content": content}
    return {"role": "assistant", "content": [{"type": "text", "text": str(content)}]}


def _call_agent_rest(question: str, history: list | None = None, stream: bool = False) -> dict:
    """
    Call Cortex Agent via REST API.
    Auth: Bearer token. Endpoint: POST .../databases/{db}/schemas/{schema}/agents/{name}:run
    """
    account = os.getenv("SNOWFLAKE_ACCOUNT", "").strip()
    token = os.getenv("SNOWFLAKE_TOKEN", "").strip()
    database = os.getenv("SNOWFLAKE_DATABASE", "FROSTY")
    schema = os.getenv("DBT_SCHEMA", "DBT_DEV_DBT_DEV")
    if not account or not token:
        raise ValueError("SNOWFLAKE_ACCOUNT and SNOWFLAKE_TOKEN required for REST API")

    # Build base URL: https://{account}.snowflakecomputing.com
    if ".snowflakecomputing.com" in account:
        base = account if account.startswith("https://") else f"https://{account}"
    else:
        base = f"https://{account}.snowflakecomputing.com"

    url = f"{base}/api/v2/databases/{database}/schemas/{schema}/agents/{AGENT_NAME}:run"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream" if stream else "application/json",
    }
    messages = []
    for msg in (history or []):
        messages.append(_msg_to_api_format(msg))
    body = {
        "messages": messages,
        "stream": stream,
    }
    timeout = 120
    logger.info("[Agent] REST %s: %s", url, question[:80])
    resp = requests.post(url, headers=headers, json=body, timeout=timeout, stream=stream)
    resp.raise_for_status()
    if stream:
        return _parse_sse_stream(resp)
    data = resp.json()
    if isinstance(data, dict) and "message" in data and "content" not in data:
        err = data.get("message", "Unknown error")
        code = data.get("code") or data.get("error_code")
        raise ValueError(f"Cortex Agent error{f' ({code})' if code else ''}: {err}")
    return data


def _parse_sse_stream(response: requests.Response) -> dict:
    """Parse SSE stream; return last event with full content or aggregate."""
    last_response: dict | None = None
    for line in response.iter_lines(decode_unicode=True):
        if not line or not line.startswith("data: "):
            continue
        raw = line[6:].strip()
        if raw == "[DONE]" or not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            if "content" in data and isinstance(data.get("content"), list):
                last_response = {"role": "assistant", "content": data["content"]}
            elif "message" in data and last_response is None:
                last_response = {
                    "role": "assistant",
                    "content": [{"type": "text", "text": str(data.get("message", ""))}],
                }
    return last_response or {"role": "assistant", "content": [{"type": "text", "text": ""}]}


def stream_agent_rest_generator(question: str, history: list | None = None):
    """
    Call Cortex Agent via REST with stream=True; yield accumulated content list for each SSE event.
    Used by the chat UI for live thinking/SQL/results. Raises on auth/network errors.
    """
    account = os.getenv("SNOWFLAKE_ACCOUNT", "").strip()
    token = os.getenv("SNOWFLAKE_TOKEN", "").strip()
    database = os.getenv("SNOWFLAKE_DATABASE", "FROSTY")
    schema = os.getenv("DBT_SCHEMA", "DBT_DEV_DBT_DEV")
    if not account or not token:
        raise ValueError("SNOWFLAKE_ACCOUNT and SNOWFLAKE_TOKEN required for REST API")
    if ".snowflakecomputing.com" in account:
        base = account if account.startswith("https://") else f"https://{account}"
    else:
        base = f"https://{account}.snowflakecomputing.com"
    url = f"{base}/api/v2/databases/{database}/schemas/{schema}/agents/{AGENT_NAME}:run"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
    }
    messages = []
    for msg in (history or []):
        messages.append(_msg_to_api_format(msg))
    body = {"messages": messages, "stream": True}
    timeout = 120
    logger.info("[Agent] REST stream: %s", question[:80])
    resp = requests.post(url, headers=headers, json=body, timeout=timeout, stream=True)
    resp.raise_for_status()
    last_content: list = []
    for line in resp.iter_lines(decode_unicode=True):
        if not line or not line.startswith("data: "):
            continue
        raw = line[6:].strip()
        if raw == "[DONE]" or not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and "content" in data and isinstance(data["content"], list):
            last_content = data["content"]
            yield last_content
    if last_content:
        yield last_content


def _call_agent_via_service(question: str, history: list | None = None) -> dict:
    """
    Call agent via dedicated agent service (separate process).
    Use when AGENT_SERVICE_URL is set — avoids queueing behind Dash report callbacks.
    """
    url = os.getenv("AGENT_SERVICE_URL", "").strip()
    if not url:
        raise ValueError("AGENT_SERVICE_URL not set")
    run_url = url.rstrip("/") + "/run"
    body = {"question": question, "history": history or []}
    timeout = 120
    logger.info("[Agent] Calling dedicated service %s", run_url)
    resp = requests.post(run_url, json=body, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data and "content" not in data:
        raise ValueError(data.get("error", "Service error"))
    return data


def can_use_stream() -> bool:
    """True when Cortex REST API is used (no agent service) so the UI can stream deltas."""
    if os.getenv("AGENT_SERVICE_URL"):
        return False
    return bool(USE_REST and os.getenv("SNOWFLAKE_ACCOUNT") and os.getenv("SNOWFLAKE_TOKEN"))


def call_agent(question: str, history: list | None = None) -> dict:
    """
    Send a question to the Snowflake Cortex Agent.
    When AGENT_SERVICE_URL is set, calls the dedicated agent service (no queueing).
    Otherwise prefers REST API when AGENT_USE_REST_API=true; falls back to DATA_AGENT_RUN SQL.
    Returns the raw response (role, content array, metadata).
    When using REST, stream=True so responses are aggregated from SSE.
    Supports multi-turn: pass history from dcc.Store.
    """
    if os.getenv("AGENT_SERVICE_URL"):
        try:
            return _call_agent_via_service(question, history)
        except Exception as e:
            logger.warning("[Agent] Service failed, falling back to direct call: %s", e)

    if USE_REST and os.getenv("SNOWFLAKE_ACCOUNT") and os.getenv("SNOWFLAKE_TOKEN"):
        try:
            return _call_agent_rest(question, history, stream=USE_STREAM)
        except Exception as e:
            logger.warning("[Agent] REST failed, falling back to SQL: %s", e)

    database = os.getenv("SNOWFLAKE_DATABASE", "FROSTY")
    schema = os.getenv("DBT_SCHEMA", "DBT_DEV_DBT_DEV")
    agent_fqn = f"{database}.{schema}.{AGENT_NAME}"

    messages = []
    for msg in (history or []):
        messages.append(_msg_to_api_format(msg))

    body = {
        "messages": messages,
        "stream": False,
    }
    body_json = json.dumps(body)
    AGENT_TIMEOUT_SECONDS = 120

    logger.info("[Agent] Calling %s via DATA_AGENT_RUN: %s", agent_fqn, question[:80])
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {AGENT_TIMEOUT_SECONDS}"
            )
            cursor.execute(
                "SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(%s, %s) AS resp",
                [agent_fqn, body_json],
                timeout=AGENT_TIMEOUT_SECONDS,
            )
            row = cursor.fetchone()
    except Exception as e:
        logger.exception("[Agent] DATA_AGENT_RUN failed: %s", e)
        raise

    if not row or not row[0]:
        raise ValueError("Agent returned empty response")
    resp = row[0]
    if isinstance(resp, str):
        resp = json.loads(resp)
    if isinstance(resp, dict) and "message" in resp and "content" not in resp:
        err = resp.get("message", "Unknown error")
        code = resp.get("code") or resp.get("error_code")
        raise ValueError(f"Cortex Agent error{f' ({code})' if code else ''}: {err}")
    logger.info("[Agent] DATA_AGENT_RUN completed successfully")
    return resp
