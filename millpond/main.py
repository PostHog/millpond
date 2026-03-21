import logging
import signal
import sys

from millpond import logging_config, server

log = logging.getLogger(__name__)


def main():
    logging_config.setup()
    log.info("millpond starting")

    http = server.start()
    server.health.mark_started()

    shutdown = False

    def on_signal(signum, _frame):
        nonlocal shutdown
        log.info("Received signal %s, shutting down", signal.Signals(signum).name)
        shutdown = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    # TODO: main loop goes here
    log.info("millpond ready")

    try:
        signal.pause()
    except KeyboardInterrupt:
        pass

    log.info("millpond shutdown complete")
    http.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
