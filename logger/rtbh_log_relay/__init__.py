import logging

local_logger = logging.getLogger(__name__)


def setup_logger():
    local_logger.setLevel(logging.INFO)
    if not local_logger.handlers:
        local_logger.addHandler(logging.StreamHandler())
        local_logger.handlers[-1].setFormatter(logging.Formatter("%(asctime)s %(process)5d %(levelname)s %(message)s"))
