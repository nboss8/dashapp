"""Gunicorn production config. Use unix socket for Nginx proxy."""
import os

bind = "unix:/run/dashapp/dashapp.sock"
worker_class = "sync"
workers = 4
threads = 2
timeout = 120
keepalive = 65

# Logging
accesslog = "/var/log/dashapp/access.log"
errorlog = "/var/log/dashapp/error.log"
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

# Process naming
proc_name = "dashapp"
