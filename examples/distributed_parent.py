# This is the top-level process.
import subprocess

from logger import get_rtbh_logger
from logger.scope import get_current_scope_id, new_scope

my_logger = get_rtbh_logger(__name__)


@new_scope
def function_that_runs_in_parent_process():
    my_logger.info("I am about to start a new process in the same logical scope")

    # For simplicity, I will demonstrate how to use the logger with multiple processes
    # on the same host.
    # The subprocess could be actually executed on a different host as long as the
    # RTBH_LOGGER_SCOPE_ID variable is properly set. It would work the same way.

    # Note that the subprocess will inherit not only the top-level scope, but also
    # the subscope created by the invocation of the current function
    # `function_that_runs_in_parent_process` decorated with `@new_scope`.
    subprocess.check_output(f"""
        export RTBH_LOGGER_SCOPE_ID={get_current_scope_id()}
        python3 distributed_child.py
    """, shell=True)

    my_logger.info("I am in parent process, done with my child process.")


function_that_runs_in_parent_process()
