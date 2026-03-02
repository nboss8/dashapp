"""Gunicorn entry point. Exposes the Dash app's Flask server."""
from app import app

server = app.server

if __name__ == "__main__":
    server.run(host="0.0.0.0", port=8050, debug=False)
