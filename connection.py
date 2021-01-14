import logging
import time

from thrift.protocol import TBinaryProtocol, TCompactProtocol
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport, TFramedTransport, TTransportException
from thrift.transport.THttpClient import THttpClient

from pythbase.config import TransportType, ProtocolType

logger = logging.getLogger(__name__)

THRIFT_TRANSPORTS = {
    TransportType.BUFFERED: TBufferedTransport,
    TransportType.FRAMED: TFramedTransport,
}
THRIFT_PROTOCOLS = {
    ProtocolType.BINARY: TBinaryProtocol.TBinaryProtocol,
    ProtocolType.COMPACT: TCompactProtocol.TCompactProtocol,
}


class Connection(object):
    """
    Class to manage basic transport and protocol.
    User should not use instances of this class directly.
    The instances should be managed by a Client object.

    """
    def __init__(self, host,
                 port,
                 transport_type,
                 protocol_type,
                 retry_timeout,
                 retry_times,
                 use_ssl,
                 use_http,
                 hide_http_port):

        self.host = host
        self.port = port
        self.use_ssl = use_ssl
        self.use_http = use_http
        self.hide_http_port = hide_http_port

        self._transport_type = THRIFT_TRANSPORTS[transport_type]
        self._protocol_type = THRIFT_PROTOCOLS[protocol_type]
        self._retry_timeout = retry_timeout
        self._retry_times = retry_times
        self._rebuild_protocol()
        self._initialized = True

    def _rebuild_protocol(self):
        """
        @Deprecated
        Rebuild the transport, protocol from the configuration.
        Should not be used directly by users.
        Returns:
            None
        """
        if self.use_http:
            # if use http transport,
            prefix = 'https://' if self.use_ssl else 'http://'
            if self.hide_http_port:
                self.transport = THttpClient(uri_or_host=prefix + self.host)
            else:
                self.transport = THttpClient(uri_or_host=prefix + self.host + ':' + str(self.port) + '/thrift.jsp')
            # http only support binary protocol
            self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
            return

        if self.use_ssl:
            from thrift.transport.TSSLSocket import TSSLSocket
            socket = TSSLSocket(host=self.host, port=self.port)
        else:
            socket = TSocket(host=self.host, port=self.port)
        self.transport = self._transport_type(socket)
        self.protocol = self._protocol_type(self.transport)

    def is_open(self):
        return self.transport.isOpen()

    def open(self):
        if self.transport.isOpen():
            return
        logger.debug("Opening thrift transport throught TCP connection.")
        self.transport.open()

    def close(self):
        if not self.transport.isOpen():
            return
        if logger is not None:
            logger.debug("Closing thrift transport to {}:{}.".format(self.host, self.port))
        self.transport.close()

    def reconnect(self):
        """
        @Deprecated
        Method to rebuild the connection with thrift server. Should not be used by the user directly.
        Returns: None

        """
        if not self.transport.isOpen():
            logger.info("Connection lose is detected and start reconnecting to the target thrift server.")
            for i in range(self._retry_times):
                if self.transport.isOpen():
                    logger.info("Reconnection success after retrying {} times.".format(i))
                    return True
                self._rebuild_protocol()
                try:
                    logger.info("Starting reconnection to thrift server.")
                    self.transport.open()
                    logger.info("Reconnection success after retrying {} times.".format(i + 1))
                    return True
                except TTransportException:
                    logger.error("Reconnected {} times but failed.".format(i + 1))
                    time.sleep(self._retry_timeout)
            if not self.transport.isOpen():
                logger.error("Failed to rebuild connection with target thrift server.")
                raise TTransportException(type=TTransportException.NOT_OPEN,
                                          message="Failed to rebuild connection with target thrift server.")
            return False
