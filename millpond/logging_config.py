import logging
import os
import sys


def setup() -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    pod_name = os.environ.get("POD_NAME") or os.environ.get("HOSTNAME", "")
    # Extract ordinal from pod name (e.g. "millpond-events-2" -> "2")
    ordinal = pod_name.rsplit("-", 1)[-1] if "-" in pod_name else pod_name
    prefix = f"[{ordinal}]" if ordinal else ""
    logging.basicConfig(
        level=level,
        format=f"%(asctime)s %(levelname)-5s {prefix}[%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )
    # Quiet noisy libraries
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)
