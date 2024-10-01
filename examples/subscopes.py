from logger import get_rtbh_logger
from logger.scope import manual_scope, new_scope

my_logger = get_rtbh_logger(__name__)


# subscope can be created for each function invocation
# if the function is decorated with `new_scope`
@new_scope
def function_in_a_subscope():
    import time
    time.sleep(1)  # Duration of the subscope will be also recorded and presented in the logs viewer.


function_in_a_subscope()

# there is also a context manager for creating a subscope
# that is not aligned with a function invocation boundary
with manual_scope("scope for only a couple of statements"):
    my_logger.info("I am now in a subscope")
    my_logger.info("Now I will exit the subscope")

my_logger.info("Now I am in the top scope again")
