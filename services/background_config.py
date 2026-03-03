"""
Background callback manager for long-running callbacks (e.g. Cortex Agent).
Uses diskcache; suitable for dev. For production with gunicorn, consider Celery+Redis.
"""
import diskcache
from dash import DiskcacheManager

_cache = diskcache.Cache("./cache")
background_callback_manager = DiskcacheManager(_cache)
