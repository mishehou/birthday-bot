# Gunicorn configuration
# Single gevent worker: one scheduler instance, but async so WAHA calls
# don't freeze the site while waiting for a response.
bind = "0.0.0.0:5000"
workers = 1
worker_class = "gevent"
timeout = 120
accesslog = "-"
errorlog = "-"


def post_fork(server, worker):
    """Initialise DB, register the WhatsApp instance, and start the scheduler."""
    from app import init_db, ensure_whatsapp_instance, start_scheduler
    init_db()
    ensure_whatsapp_instance()
    start_scheduler()
