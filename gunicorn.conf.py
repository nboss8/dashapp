"""Gunicorn production config. Use unix socket for Nginx proxy."""
import os

bind = os.getenv("GUNICORN_BIND", "unix:/run/dashapp/dashapp.sock")
worker_class = "sync"
workers = 1  # Must be 1 - cache_manager uses process-local globals
threads = 8  # Handle concurrency via threads, not workers
timeout = 120
keepalive = 65

# Logging
accesslog = os.getenv("GUNICORN_ACCESSLOG", "/var/log/dashapp/access.log")
errorlog = os.getenv("GUNICORN_ERRORLOG", "/var/log/dashapp/error.log")
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

# Process naming
proc_name = "dashapp"
