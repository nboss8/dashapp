"""
Standalone agent service — runs Cortex Agent in a dedicated process.
Use this so chat never queues behind report callbacks in the main Dash app.

Run: python agent_service.py
  or: uvicorn agent_service:app --host 0.0.0.0 --port 8051

Set AGENT_SERVICE_URL=http://localhost:8051 in the main app to use this service.
"""
import logging
import os

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

try:
    from flask import Flask, request, jsonify

    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False

app = Flask(__name__) if FLASK_AVAILABLE else None


def _run_agent():
    from services.snowflake_agent import call_agent

    data = request.get_json(force=True, silent=True) or {}
    question = (data.get("question") or "").strip()
    history = data.get("history") or []
    if not question and not (history and history[-1].get("role") == "user"):
        return jsonify({"error": "question required"}), 400
    if not question and history:
        last = history[-1]
        if last.get("role") == "user":
            c = last.get("content", "")
            question = c if isinstance(c, str) else (c[0].get("text", "") if c and isinstance(c[0], dict) else str(c or ""))
    if not question:
        return jsonify({"error": "question required"}), 400
    try:
        resp = call_agent(question, history)
        return jsonify(resp)
    except Exception as e:
        logger.exception("Agent call failed")
        return jsonify({"error": str(e), "content": [{"type": "text", "text": f"Error: {e}"}]}), 500


if FLASK_AVAILABLE:
    app.route("/run", methods=["POST"])(_run_agent)

    @app.route("/health")
    def health():
        return "OK", 200


def main():
    port = int(os.getenv("AGENT_SERVICE_PORT", "8051"))
    logger.info("Agent service starting on port %s", port)
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
