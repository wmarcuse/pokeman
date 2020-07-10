from pokeman.coatings.messaging_endpoints._abc_endpoint import AbstractBasicMessagingEndpoint


class SelectiveConsumer(AbstractBasicMessagingEndpoint):
    """
    Basic Message Enterprise Integration Pattern for messaging
    endpoint > Selective Consumer.

    A Sentinel is declared default for various pattern parameters,
    as the possible None declaration is a posteriori.
    """
    _SENTINEL = object()

    def __init__(
            self,
            exchange_name=_SENTINEL,
            queue=_SENTINEL,
            callback_method=_SENTINEL,
            qos=_SENTINEL
    ):
        self.exchange_name = self._exchange_name_setter(exchange_name=exchange_name)
        self.queue = self._queue_setter(queue=queue)
        self.callback_method = self._callback_method_setter(callback_method=callback_method)
        self.qos = self._qos_setter(qos=qos)

    def _exchange_name_setter(self, exchange_name):
        if exchange_name is self._SENTINEL:
            return "pokeman_exchange"
        else:
            return exchange_name

    def _queue_setter(self, queue):
        if queue is self._SENTINEL:
            return "pokeman_queue"
        else:
            return queue

    def _callback_method_setter(self, callback_method):
        if callback_method is self._SENTINEL:
            return lambda: 'No callback method provided'
        else:
            return callback_method

    def _qos_setter(self, qos):
        if qos is self._SENTINEL:
            return 1
        else:
            return qos
