from typing import List, Union
from pythbase.hbase.ttypes import TGet, TDelete, TScan, TPut, TColumnValue, TColumn


class Operation(object):
    """
    Basic object represent a single HBase operation.
    """
    def __init__(self, row,  # type: Union[None, str]
                 family,  # type: Union[None, str]
                 qualifier,  # type: Union[None, str]
                 value,  # type: Union[None, str]
                 ):
        self.row = row
        self.family = family
        self.qualifier = qualifier
        self.value = value


class Get(Operation):

    def __init__(self,
                 row=None,  # type: Union[None, str]
                 family=None,  # type: Union[None, str]
                 qualifier=None,  # type: Union[None, str, List[str]]
                 value=None,  # type: Union[None, str]
                 max_versions=None  # type: Union[None, int]
                 ):
        super(Get, self).__init__(row, family, qualifier, value)
        self.maxVersions = max_versions
        self.core = TGet(row=self.row,
                         columns=_column_format(family, qualifier),
                         timestamp=None,
                         timeRange=None, maxVersions=self.maxVersions)


class Delete(Operation):

    def __init__(self, row,  # type: Union[None, str]
                 family=None,  # type: Union[None, str]
                 qualifier=None,  # type: Union[None, str]
                 value=None,  # type: Union[None, str]
                 ):
        super(Delete, self).__init__(row, family, qualifier, value)
        self.core = TDelete(
            row=row,
            columns=_column_format(self.family, self.qualifier),
        )


class Scan(Operation):

    def __init__(self, start_row,  # type: Union[None, str]
                 family=None,  # type: Union[None, str]
                 qualifier=None,  # type: Union[None, str, List[str]]
                 stop_row=None,  # type: Union[None, str]
                 num_rows=10000,  # type: int
                 max_versions=None,  # type: Union[None, int]
                 reversed=None,  # type: Union[None, bool]
                 ):
        super(Scan, self).__init__(start_row, family, qualifier, None)
        self.reversed = reversed
        self.stop_row = stop_row
        self.num_rows = num_rows
        self.core = TScan(
            startRow=self.row,
            stopRow=self.stop_row,
            columns=_column_format(self.family, self.qualifier),
            maxVersions=max_versions,
            reversed=self.reversed,
        )


class Put(Operation):

    def __init__(self, row,  # type: Union[None, str]
                 family,  # type: Union[None, str]
                 qualifier,  # type: Union[None, str, List[str]]
                 value,  # type: Union[str, List[str]]
                 ):
        if not family:
            raise ValueError("Family must be given when doing put operation.")
        super(Put, self).__init__(row, family, qualifier, value)
        column_values = []
        columns = _column_format(self.family, self.qualifier)
        if isinstance(value, str):
            for col in columns:
                column_values.append(TColumnValue(
                    family=col.family,
                    qualifier=col.qualifier,
                    value=value
                ))
        elif isinstance(value, list) or isinstance(value, tuple):
            if len(columns) != len(value):
                raise ValueError("The number of columns mismatches the number of value list.")
            for i, col in enumerate(columns):
                column_values.append(TColumnValue(
                    family=col.family,
                    qualifier=col.qualifier,
                    value=value[i]
                ))
        self.core = TPut(
            row=self.row,
            columnValues=column_values
        )


def _column_format(family, qualifier):
    # type: (str, Union[None, str, List[str]]) -> Union[None, List[TColumn]]
    """
    Util method to get columns from given column family and qualifier.
    If the family is None, this method will return None.
    Args:
        family: name of column family.
        qualifier: name of column qualifier, it can be a str, None or a list of strs.

    Returns: a list of combined columns.

    """
    if family is None:
        return None
    if not isinstance(family, str):
        raise ValueError("A family name must be a str object, but got {}".format(type(family)))

    if qualifier is None:
        return [TColumn(family=family)]
    if isinstance(qualifier, str):
        return [TColumn(family=family, qualifier=qualifier)]
    if isinstance(qualifier, list) or isinstance(qualifier, tuple):
        cols = []
        for cq in qualifier:
            if isinstance(cq, str) or cq is None:
                cols.append(TColumn(family=family, qualifier=cq))
            else:
                raise ValueError("Qualifier should be None, str or a list (tuple) of str")
