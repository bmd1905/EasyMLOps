import logging
import sys

logger = logging.getLogger(__name__)


def patch_logging_proxy():
    """Patch logging proxy objects to handle encoding attribute"""
    # Get the actual LoggingProxy class from the stdout/stderr if they are proxied
    logging_proxy_class = None

    if hasattr(sys.stdout, "__class__"):
        if sys.stdout.__class__.__name__ == "LoggingProxy":
            logging_proxy_class = sys.stdout.__class__

    if not logging_proxy_class and hasattr(sys.stderr, "__class__"):
        if sys.stderr.__class__.__name__ == "LoggingProxy":
            logging_proxy_class = sys.stderr.__class__

    # Patch the LoggingProxy class if found
    if logging_proxy_class and not hasattr(logging_proxy_class, "encoding"):
        setattr(logging_proxy_class, "encoding", "utf-8")
        logger.debug("Added encoding attribute to LoggingProxy class")

    # Always patch stdout/stderr if they're missing encoding
    for stream in (sys.stdout, sys.stderr):
        if not hasattr(stream, "encoding"):
            setattr(stream, "encoding", "utf-8")
            logger.debug(f"Added encoding attribute to {stream}")
