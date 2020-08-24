from inspect import getfullargspec

from pokeman.coatings.messaging_endpoints._abc_endpoint import AbstractBasicMessagingEndpoint
from pokeman.amqp_resources.globals import Exchange


class PollingConsumer(AbstractBasicMessagingEndpoint):
    """
    Basic Message Enterprise Integration Pattern for messaging
    endpoint > Polling Consumer.

    A Sentinel is declared default for various pattern parameters,
    as the possible None declaration is a posteriori.
    """
    _SENTINEL = Ellipsis

    def __init__(
            self,
            exchange=_SENTINEL,
            queue=_SENTINEL,
            callback_method=_SENTINEL,
            qos=_SENTINEL
    ):
        """
        :param app_id: The provided app_id
        :type app_id: str

        :param exchange: The provided exchange.
        :type exchange: Exchange

        :param routing_key: The provided routing key.
        :type routing_key: pokeman.RoutingKey
        """
        self._exchange = exchange
        self._exchange_converter = lambda _pkid: Exchange(
            specific_poker=self._specific_poker_setter(
                specific_poker=_pkid,
                lambda_call=True
            )).exchange_name if self._exchange is self._SENTINEL else self._exchange.exchange_name
        self.exchange = self._exchange_setter(exchange=self._exchange_converter)
        self.queue = self._queue_setter(queue=queue)
        self.callback_method = self._callback_method_setter(callback_method=callback_method)
        self.qos = self._qos_setter(qos=qos)

    def _exchange_setter(self, exchange):
        return exchange

    def _exchange_name_setter(self, exchange_name):
        if exchange_name is self._SENTINEL:
            return "pokeman_exchange"
        else:
            return exchange_name

    def _queue_setter(self, queue):
        if queue is self._SENTINEL:
            return "pokeman_queue"
        else:
            return queue.queue_name

    def _callback_method_setter(self, callback_method):
        if callback_method is self._SENTINEL:
            return lambda body: 'No callback method provided'
        else:
            obligatory_args = ['body', 'properties']
            for arg in obligatory_args:
                if arg not in getfullargspec(callback_method).args:
                    raise AttributeError("The provided callback method needs to have the {ARG} argument".format(
                        ARG=arg
                    ))
            return callback_method

    def _qos_setter(self, qos):
        if qos is self._SENTINEL:
            return 1
        else:
            return qos

    def _specific_poker_setter(self, specific_poker, lambda_call=False):
        if specific_poker is self._SENTINEL:
            return None
        elif lambda_call is False:
            return specific_poker.poker_id
        elif lambda_call is True:
            return specific_poker
        else:
            raise ValueError('Unresolvable specific poker parameter')