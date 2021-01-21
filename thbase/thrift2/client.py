"""
Copyright 2021 Yutong Sean

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from thbase.hbase import THBaseService
from thbase.clientbase import ClientBase
from thbase.thrift2.table import Table
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

    def _put_row(self, **kwargs):
        """
        Private method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        table_name = kwargs['table_name']
        put = kwargs['put']
        self.client.put(table_name, put.core)
        return True

    def _put_rows(self, **kwargs):
        """
        Private method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        table_name = kwargs['table_name']
        puts = kwargs['puts']
        self.client.putMultiple(table_name, [put.core for put in puts])
        return True

    def _get_row(self, **kwargs):
        """
        Private method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        table_name = kwargs['table_name']
        get = kwargs['get']
        result = self.client.get(table_name, get.core)
        return [result]

    def _get_rows(self, **kwargs):
        """
        Private method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        table_name = kwargs['table_name']
        gets = kwargs['gets']
        return self.client.getMultiple(table_name, [get.core for get in gets])

    def _scan(self, **kwargs):
        """
        Private method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        table_name = kwargs['table_name']
        scan = kwargs['scan']
        result = self.client.getScannerResults(table_name, scan.core, scan.num_rows)
        return result

    def _delete_row(self, **kwargs):
        """
        Private method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        table_name = kwargs['table_name']
        delete = kwargs['delete']
        self.client.deleteSingle(table_name, delete.core)

    def _delete_batch(self, **kwargs):
        """
        Private method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        table_name = kwargs['table_name']
        deletes = kwargs['deletes']
        self.client.deleteMultiple(table_name, [delete.core for delete in deletes])

    def _refresh_client(self):
        """
        Private method, should not be used by users.
        Args:
            **kwargs:

        Returns:

        """
        self.client = THBaseService.Client(self.connection.protocol)

    def get_table(self, table_name):
        """
        Acquire a table object to use functional methods.
        Args:
            **kwargs:

        Returns:

        """
        return Table(table_name=table_name, client=self)




