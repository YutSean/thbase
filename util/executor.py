from handlers import ExceptionHandler, MessageType
from pythbase.thrift2.hbase.ttypes import TException

import logging
import time

logger = logging.getLogger(__name__)


class Executor(object):
    def __init__(self, retry_times, retry_timeout, master):
        self._retry_times = retry_times
        self._retry_timeout = retry_timeout
        self._master = master
        self.handler = ExceptionHandler(self._master)

    def call(self, func):
        """
        Execute a specific function with given parameters.
        If the system meets an exception the exception handler will try to handle it.
        If the exception is critical and can not be handled, it will be raised and interrupt the system.
        If the exception is acceptable, the system will retry the operation for several times (set in config).
        After retrying, if the system still cannot get a valid value,
        the function will be marked as failed and return a False.
        Args:
            func: A callable function.

        Returns:
            For Put, Delete operations: True if success, False otherwise.
            For Get, Scan operations: A list of results, False otherwise.
        """
        if not callable(func):
            raise ValueError("A callable function needed here but got {}.".format(type(func)))
        for i in range(self._retry_times + 1):
            try:
                result = func()
                # if result is None, the func is Put, or Delete,
                # so should return a bool to represent if the operation successes.
                if result is None:
                    return True
                # when result is not None, it contains a list of TResult objects with Get and Scan operation.
                # so here return the list directly.
                return result
            except TException as e:
                if not self.handler.handle(MessageType.ERROR, value=e):
                    logger.error("There occurs an error that can not be handled. {}. "
                                 "System is shutdown.".format(e.message))
                    raise e
                else:
                    logger.warn("An error occurs, {}. Redo the operation after {} seconds.".
                                format(e.message, self._retry_timeout))
            time.sleep(self._retry_timeout)
        # this False means the operation failed after retry N times.
        return False
