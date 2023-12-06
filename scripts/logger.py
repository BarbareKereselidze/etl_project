import logging


def get_logger():
    """
    setting up a logger function to capture errors INFO level and above.

    Returns:
        * configured logger
    """

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    return logger

