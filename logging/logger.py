import logging


def get_logger() -> logging.Logger:
    """ setting up a logger function to capture errors INFO level and above """

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    return logger

