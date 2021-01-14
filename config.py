from enum import Enum

TransportType = Enum('TransportType', ['FRAMED', 'BUFFERED'])
ProtocolType = Enum('ProtocolType', ['BINARY', 'COMPACT'])

HOST_DEFAULT = 'localhost'
PORT_DEFAULT = 9090
TRANSPORT_DEFAULT = TransportType.FRAMED
PROTOCOL_DEFAULT = ProtocolType.BINARY
RETRY_TIMEOUT_DEFAULT = 1
RETRY_TIMES_DEFAULT = 10
BATCH_SIZE_DEFAULT = 10
USE_SSL_DEFAULT = False
USE_HTTP_DEFAULT = False
RECONNECTION_TIMES = 10
RECONNECTION_TIMEOUT = 10
HIDE_HTTP_PORT_DEFAULT = False


class ClientConfig(object):
    def __init__(self, thrift_host=HOST_DEFAULT,  # type: str
                 port=PORT_DEFAULT,  # type: int
                 retry_timeout=RETRY_TIMES_DEFAULT,  # type: int
                 retry_times=RETRY_TIMES_DEFAULT,  # type: int
                 connection_retry_times=RECONNECTION_TIMES,  # type: int
                 connection_retry_timeout=RECONNECTION_TIMEOUT,  # type: int
                 transport_type=TRANSPORT_DEFAULT,  # type: TransportType
                 protocol_type=PROTOCOL_DEFAULT,  # type: ProtocolType
                 use_ssl=USE_SSL_DEFAULT,  # type: bool
                 batch_size=BATCH_SIZE_DEFAULT,  # type: int
                 use_http=USE_HTTP_DEFAULT,  # type: bool
                 hide_http_port=HIDE_HTTP_PORT_DEFAULT,  # type: bool
                 ):
        """
        Basic client configuration.
        Args:
            thrift_host: thrift server address
            port: thrift server port
            retry_timeout: time interval between two retries for operation. Unit is seconds.
            retry_times: operation retry times. If the operation is still unsuccessful after being
            retried for this times, it will return False.
            connection_retry_times: time interval between two retries for reconnection. Unit is seconds.
            connection_retry_timeout: reconnection retry times. If the client still cannot rebuild the connection
            after retrying for this times, an exception will be raised.
            transport_type: Up to now, the server support "framed", "buffered" and "http" transport.
            protocol_type: Up to now, the server support "Binary" and "Compact" protocol.
            use_ssl: if the client use SSL to secure the connection. Only support http transport and binary protocol.
            batch_size: the max size of the batch when using batch operation in a Table object.
            Batch size can be customized with given setter method.
            use_http: if the client use http as the transport but not TCP.
        """
        self._host = thrift_host
        self._port = port
        self._connection_retry_timeout = connection_retry_timeout
        self._connection_retry_times = connection_retry_times
        self._retry_timeout = retry_timeout
        self._retry_times = retry_times
        self._transport_type = transport_type
        self._protocol_type = protocol_type
        self._batch_size = batch_size
        self._use_ssl = use_ssl
        self._use_http = use_http
        self._hide_http_port = hide_http_port
        self._parametereter_check()

    def _parametereter_check(self):
        if not isinstance(self.port, int) or self.port not in range(65536):
            raise ValueError("Port must be an integer in 0~65535.")
        if not isinstance(self.retry_times, int) or self.retry_times < 1:
            raise ValueError("Retry times for connection rebuild must be a positive integer.")
        if not isinstance(self.retry_timeout, int) or self.retry_timeout < 1:
            raise ValueError("Time interval for connection rebuild must be a positive integer.")
        if not isinstance(self.batch_size, int) or self.batch_size < 1:
            raise ValueError("Batch size must be a positive integer.")
        if not isinstance(self.use_ssl, bool):
            raise ValueError("Parameter use_ssl must be a bool value.")
        if not isinstance(self.use_http, bool):
            raise ValueError("Parameter use_http must be a bool value.")
        if not isinstance(self.hide_http_port, bool):
            raise ValueError("Parameter hide_http_port must be a bool value.")
        if self._transport_type not in TransportType:
            raise ValueError("Invalid type of transport {}. Use one of the specific enum type {}."
                             .format(type(self._transport_type), ', '.join([str(a) for a in TransportType])))
        if self._protocol_type not in ProtocolType:
            raise ValueError("Invalid type of protocol {}. Use one of the specific enum type {}."
                             .format(type(self._protocol_type), ', '.join([str(a) for a in ProtocolType])))

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def connection_retry_timeout(self):
        return self._connection_retry_timeout

    @property
    def connection_retry_times(self):
        return self._connection_retry_times

    @property
    def transport_type(self):
        return self._transport_type

    @property
    def protocol_type(self):
        return self._protocol_type

    @property
    def batch_size(self):
        return self._batch_size

    @property
    def use_ssl(self):
        return self._use_ssl

    @property
    def use_http(self):
        return self._use_http

    @property
    def retry_times(self):
        return self._retry_times

    @property
    def retry_timeout(self):
        return self._retry_timeout

    @property
    def hide_http_port(self):
        return self._hide_http_port

    @batch_size.setter
    def batch_size(self, value):
        if not isinstance(value, int) or value <= 0:
            raise ValueError("Batch size must be an positive Integer.")
        self._batch_size = value
