from typing import Union
from pythbase.util.bytes import to_str


class Cell(object):

    def __init__(self, table_name,  # type: Union[None, bytes]
                 row,  # type: Union[None, bytes]
                 family,  # type: Union[None, bytes]
                 qualifier,  # type: Union[None, bytes]
                 value,  # type: Union[None, bytes]
                 timestamp,  # type: Union[None, int]
                 ):
        """
        Data structure to load data from hbase. If there is no matched cell or some errors occur at hbase server,
        all the attributes could be None. In python2, bytes are represented by str, so the type of value is str.
        Args:
            table_name: name of the table.
            row: the row key.
            family: the column family.
            qualifier: the column qualifier.
            value: the bytes stored in the cell.
            timestamp: a long int.
        """
        self._table_name = table_name
        self._row = row
        self._family = family
        self._qualifier = qualifier
        self._value = value
        self._timestamp = timestamp

    @property
    def table_name(self):
        return self._table_name

    @property
    def row(self):
        return self._row

    @property
    def family(self):
        return self._family

    @property
    def qualifier(self):
        return self._qualifier

    @property
    def value(self):
        return self._value

    @property
    def timestamp(self):
        return self._timestamp

    def __str__(self):
        return ":".join([to_str(self.table_name), to_str(self.row), to_str(self.family), to_str(self.qualifier)]) + \
               ' => ' + to_str(self.value)
