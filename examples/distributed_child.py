# Setup is identical in child_process and in parent_process.
#
# This script doesn't need to know if it is run as top-level or
# as a child script from the logger's perspective.
#
# The environment variable RTBH_LOGGER_SCOPE_ID will be automatically
# detected and respected by our logger handler.
from logger import get_rtbh_logger
from logger.scope import new_scope

my_logger = get_rtbh_logger(__name__)

my_logger.info("Hello in child process")


@new_scope
def function_that_runs_in_child_process():
    my_logger.info("Hello in a child process in a subscope")


function_that_runs_in_child_process()
