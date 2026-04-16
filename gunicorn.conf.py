# Gunicorn configuration
# Single worker so only one scheduler instance runs
bind = "0.0.0.0:5000"
workers = 1
timeout = 120
accesslog = "-"
errorlog = "-"


def post_fork(server, worker):
    """Initialise DB, register the WhatsApp instance, and start the scheduler."""
    from app import init_db, ensure_whatsapp_instance, start_scheduler
    init_db()
    ensure_whatsapp_instance()
    start_scheduler()
