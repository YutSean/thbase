from pythbase.thrift2.hbase import THBaseService
from pythbase.clientbase import ClientBase
from pythbase.thrift2.table import Table
import logging

logger = logging.getLogger(__name__)


class Client(ClientBase):
    """
    Client implemented by thrift2 API.
    User should not invoke methods of a Client object directly.
    This class does not provide retry mechanism, reconnection mechanism and exception handling.
    Please not use it directly.
    """
    def __init__(self, conf):
        super(Client, self).__init__(conf=conf)
        self.client = THBaseService.Client(self.connection.protocol)

    def put_row(self, **kwargs):
        """
        Deprecated. Internal method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        table_name = kwargs['table_name']
        put = kwargs['put']
        self.client.put(table_name, put.core)
        return True

    def put_rows(self, **kwargs):
        """
        Deprecated. Internal method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        table_name = kwargs['table_name']
        puts = kwargs['puts']
        self.client.putMultiple(table_name, [put.core for put in puts])
        return True

    def get_row(self, **kwargs):
        """
        Deprecated. Internal method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        table_name = kwargs['table_name']
        get = kwargs['get']
        result = self.client.get(table_name, get.core)
        return [result]

    def get_rows(self, **kwargs):
        """
        Deprecated. Internal method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        table_name = kwargs['table_name']
        gets = kwargs['gets']
        return self.client.getMultiple(table_name, [get.core for get in gets])

    def scan(self, **kwargs):
        """
        Deprecated. Internal method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        table_name = kwargs['table_name']
        scan = kwargs['scan']
        result = self.client.getScannerResults(table_name, scan.core, scan.num_rows)
        return result

    def delete_row(self, **kwargs):
        """
        Deprecated. Internal method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        table_name = kwargs['table_name']
        delete = kwargs['delete']
        self.client.deleteSingle(table_name, delete.core)

    def delete_batch(self, **kwargs):
        """
        Deprecated. Internal method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        table_name = kwargs['table_name']
        deletes = kwargs['deletes']
        self.client.deleteMultiple(table_name, [delete.core for delete in deletes])

    def refresh_client(self):
        """
        Deprecated. Internal method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        self.client = THBaseService.Client(self.connection.protocol)

    def get_table(self, table_name):
        """
        Deprecated. Internal method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        return Table(table_name=table_name, client=self)



