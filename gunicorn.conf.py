"""Allow authentication to run while another request waits on an external service.

Gunicorn loads this file automatically from the repository working directory.
Keep its configured worker count (including Render's command-line override).
Do not preload: this application starts background workers during import.
"""
worker_class = "gthread"
threads = 10
preload_app = False
# Gthread workers keep reporting liveness while requests wait on I/O. Raising
# timeout is not the remedy; keep the usual thirty-second worker watchdog.
timeout = 30
graceful_timeout = 30
accesslog = "-"
errorlog = "-"
# Include duration so Render logs distinguish slow handlers from client errors.
access_log_format = '%(h)s %(t)s "%(m)s %(U)s %(H)s" %(s)s %(b)s duration_us=%(D)s'


def on_starting(server):
    # Render's command-line --threads overrides this file's `threads` value.
    # Four threads left only two business slots and dropped the approval lane.
    # Establish the required floor before workers construct their thread pools.
    configured_threads = server.cfg.threads
    if server.cfg.worker_class.__name__ == 'ThreadWorker' and configured_threads < threads:
        server.cfg.set('threads', threads)
        server.log.warning('NP HTTP capacity floor: configured_threads=%s effective_threads=%s',
                           configured_threads, server.cfg.threads)
    server.log.info("NP HTTP V217 worker_class=%s threads=%s workers=%s preload=%s",
        server.cfg.worker_class.__name__, server.cfg.threads,
        server.cfg.workers, server.cfg.preload_app)


def post_worker_init(worker):
    from request_admission import configure_worker_capacity
    slots = configure_worker_capacity(worker.cfg.threads)
    worker.log.info("NP HTTP V220 threads=%s ordinary_request_slots=%s",
                    worker.cfg.threads, slots)
