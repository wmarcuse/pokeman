from pokeman.coatings.message_constructions._abc_message import AbstractBasicMessageConstruction
from pokeman.amqp_resources.globals import Exchange, Queue, RoutingKey


# TODO: Check topic exchange configuration
class BasicMessage(AbstractBasicMessageConstruction):
    """
    Basic Message Enterprise Integration Pattern for message 
    construction > Basic Message.

    A Sentinel is declared default for various pattern parameters,
    as the possible None declaration is a posteriori.
    """
    _SENTINEL = object()

    def __init__(
            self,
            app_id=None,
            exchange=_SENTINEL,
            routing_key=_SENTINEL
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
        self._routing_key = routing_key
        self._exchange_converter = lambda _pi: Exchange(
            specific_poker=self._specific_poker_setter(
                specific_poker=_pi,
                lambda_call=True
            )).exchange_name if self._exchange is self._SENTINEL else self._exchange.exchange_name
        self._routing_key_converter = lambda: RoutingKey().key if self._routing_key is self._SENTINEL else self._routing_key.key
        self.exchange = self._exchange_setter(exchange=self._exchange_converter)
        self.routing_key = self._routing_key_setter(routing_key=self._routing_key_converter())
        self.app_id = self._app_id_setter(app_id=app_id)

    def _app_id_setter(self, app_id):
        if app_id is self._SENTINEL:
            return "pokeman_app"
        else:
            return app_id

    def _exchange_setter(self, exchange):
        return exchange

    def _routing_key_setter(self, routing_key):
        if routing_key is None:
            raise ValueError('Please provide a routing key')
        return routing_key

    # TODO: Add Fanout and Headers exchange types

    def _specific_poker_setter(self, specific_poker, lambda_call=False):
        if specific_poker is self._SENTINEL:
            return None
        elif lambda_call is False:
            return specific_poker.poker_id
        elif lambda_call is True:
            return specific_poker
        else:
            raise ValueError('Unresolvable specific poker parameter')