from config import ClientConfig
from pythbase.connection import Connection
from pythbase.util.handlers import ExceptionHandler, MessageType
import logging

logger = logging.getLogger(__name__)


class ClientBase(object):
    """
    Abstract class for both thrift1 and thrift2 client. Implemented with Observer design pattern.
    This class uses a connection object to manage the basic thrift connection with thrift server.
    """
    def __init__(self, conf):
        """
        Initialize the thrift connection and add a new exception handler to deal with exceptions.
        This class should be used by user in NO circumstance!
        Args:
            conf: a customized ClientConfig object.
        """
        if not isinstance(conf, ClientConfig):
            err_str = "Invalid Client Configuration type {}.".format(type(conf))
            logger.error(ValueError(err_str))
            raise ValueError(err_str)
        self.conf = conf
        self.connection = Connection(host=self.conf.host,
                                     port=self.conf.port,
                                     transport_type=self.conf.transport_type,
                                     protocol_type=self.conf.protocol_type,
                                     retry_timeout=self.conf.retry_timeout,
                                     retry_times=self.conf.retry_times,
                                     use_ssl=self.conf.use_ssl,
                                     use_http=self.conf.use_http
                                     )
        self._observers = set()
        self.attach(ExceptionHandler(self))

    def attach(self, observer):
        """
        Add a new observer into the observer set.
        Args:
            observer: object watching on this client.

        Returns:

        """
        self._observers.add(observer)

    def detach(self, observer):
        """
        Remove an observer from the observer set. If the observer is not registered, do nothing.
        Args:
            observer: object in the observer set.

        Returns:

        """
        if observer in self._observers:
            self._observers.discard(observer)

    def notify(self, message_type, value):
        """
        Notify all the observer to handle something.
        Args:
            message_type: an enum defined in util.handlers module.
            value: the data for handling.

        Returns:

        """
        for obr in self._observers:
            obr.handle(message_type, value)

    def open_connection(self):
        try:
            if not self.connection.is_open():
                self.connection.open()
        except Exception as e:
            self.notify(MessageType.ERROR, e)

    def close_connection(self):
        try:
            if self.connection.is_open():
                self.connection.close()
        except Exception as e:
            self.notify(MessageType.ERROR, e)

    def put_row(self, table_name, put):
        """
        @Deprecated
        Send a single put request to thrift server. Only should be invoked by a Table object.
        Args:
            table_name: Should be invoked by a Table object.
            put: a Put object.

        Returns:
            True if the operation successes, else False.

        """
        pass

    def put_rows(self, table_name, put_list):
        """
        @Deprecated
        Send a batch of put requests to thrift server. Only should be invoked by a Table object.
        Args:
            table_name: a str representation of Table name, including the namespace part.
            put_list: a list of Put objects.

        Returns:
            True if the operation successes, else False.

        """
        pass

    def get_row(self, table_name, get):
        """
        @Deprecated
        Send a single get request to thrift server. Only should be invoked by a Table object.
        Args:
            table_name: a str representation of Table name, including the namespace part.
            get: a Get object.

        Returns:
            A list of Cells if success. An empty list if the operation fails or the target cell does not exists.
        """
        pass

    def get_rows(self, table_name, get_list):
        """
        @Deprecated
        Send a batch of get requests to thrift server. Only should be invoked by a Table object.
        Args:
            table_name: a str representation of Table name, including the namespace part.
            get_list: a list of Get objects.

        Returns:
            A list of Cells if success. An empty list if the operation fails or the target cells do not exists.
        """
        pass

    def scan(self, table_name, scan):
        """
        @Deprecated
        Send a scan request to thrift server. Only should be invoked by a Table object.
        Args:
            table_name: a str representation of Table name, including the namespace part.
            scan: a Scan object.

        Returns:
            A list of Cells if success. An empty list if the operation fails or the target cells do not exists.
        """
        pass

    def delete_row(self, table_name, delete):
        """
        @Deprecated
        Send a delete request to thrift server. Only should be invoked by a Table object.
        Args:
            table_name: a str representation of Table name, including the namespace part.
            delete: a Delete object.

        Returns:
            True if successes, else False.
        """
        pass

    def delete_batch(self, table_name, delete_list):
        """
        @Deprecated
        Send a batch of delete requests to thrift server. Only should be invoked by a Table object.
        Args:
            table_name: a str representation of Table name, including the namespace part.
            delete_list: a list of Delete objects.

        Returns:
            True if successes, else False.
        """
        pass

    def refresh_client(self):
        """
        @Deprecated
        Reconstruct a client, be used when the client reconnects to the thrift server.
        Returns:
            None
        """
        pass

    def get_table(self, table_name):
        """
        Get a Table object of given table name.
        Args:
            table_name: a str representation of Table name, including the namespace part.

        Returns:
            a Table object.
        """
        pass

