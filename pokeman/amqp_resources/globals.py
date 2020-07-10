from pokeman.utils.custom_abc import ABCMeta, abstract_attribute

import logging

LOGGER = logging.getLogger(__name__)


class AbstractExchange(metaclass=ABCMeta):
    """
    Abstract base class for Exchange object.
    """

    @abstract_attribute
    def exchange_name(self):
        pass

    @abstract_attribute
    def exchange_type(self):
        pass

    @abstract_attribute
    def durable(self):
        pass

    @abstract_attribute
    def auto_delete(self):
        pass

    @abstract_attribute
    def specific_poker(self):
        pass


class AbstractRoutingKey(metaclass=ABCMeta):
    """
    Abstract base class for RoutingKey object.
    """

    @abstract_attribute
    def key(self):
        pass


# TODO: Add arguments: arguments, callback for declaring and binding purposes
class AbstractQueue(metaclass=ABCMeta):
    """
    Abstract base class for Queue object.
    """

    @abstract_attribute
    def queue_name(self):
        pass

    @abstract_attribute
    def exchange(self):
        pass

    @abstract_attribute
    def routing_key(self):
        pass

    @abstract_attribute
    def exclusive(self):
        pass

    @abstract_attribute
    def durable(self):
        pass

    @abstract_attribute
    def auto_delete(self):
        pass

    @abstract_attribute
    def specific_poker(self):
        pass


# TODO: Add arguments: internal, arguments, callback
# TODO: Build custom passive handler
class Exchange(AbstractExchange):
    """
    Exchange object.
    """
    _SENTINEL = object()

    def __init__(self,
                 exchange_name=_SENTINEL,
                 exchange_type=_SENTINEL,
                 durable=_SENTINEL,
                 auto_delete=_SENTINEL,
                 specific_poker=_SENTINEL):
        """
        This method initialized the exchange object.

        :param exchange_name: The provided exchange name.
        :type exchange_name: str

        :param exchange_type: The provided exchange type.
        :type exchange_type: str

        :param durable: The provided durable parameter.
        :type durable: bool

        :param auto_delete: The provided auto_delete parameter.
        :type auto_delete: bool

        :param specific_poker: The provided specific poker id to handle the object.
        :type specific_poker: pokeman.Pokeman
        """
        from pokeman.amqp_resources.heapq import ResourceHeapQ  # Avoid circular import
        self.exchange_name = self._exchange_name_setter(exchange_name=exchange_name)
        self.exchange_type = self._exchange_type_setter(exchange_type=exchange_type)
        self.durable = self._durable_setter(durable=durable)
        self.auto_delete = self._auto_delete_setter(auto_delete=auto_delete)
        self.specific_poker = self._specific_poker_setter(specific_poker=specific_poker)
        ResourceHeapQ.add_resource(resource=self, specific_poker=self.specific_poker)

    def _exchange_name_setter(self, exchange_name):
        if exchange_name is self._SENTINEL:
            return 'pokeman_exchange'
        else:
            return exchange_name

    def _exchange_type_setter(self, exchange_type):
        if exchange_type is self._SENTINEL:
            return 'direct'
        else:
            return exchange_type

    def _durable_setter(self, durable):
        if durable is self._SENTINEL:
            return True
        else:
            return durable

    def _auto_delete_setter(self, auto_delete):
        if auto_delete is self._SENTINEL:
            return False
        else:
            return auto_delete

    def _specific_poker_setter(self, specific_poker):
        if specific_poker is self._SENTINEL:
            return None
        elif specific_poker is None:
            return None
        else:
            return specific_poker.poker_id


class RoutingKey(AbstractRoutingKey):
    """
    Routing key object.
    """
    _SENTINEL = object()

    def __init__(self, key=_SENTINEL):
        """
        This method initializes the routing key object.

        :param key: The provided routing key.
        :type key: str
        """
        self.key = self._routing_key_setter(key=key)

    def _routing_key_setter(self, key):
        if key is self._SENTINEL:
            return None
        else:
            return key


class Queue(AbstractQueue):
    """
    Queue object
    """
    _SENTINEL = object()

    def __init__(self,
                 queue_name=_SENTINEL,
                 exchange=_SENTINEL,
                 routing_key=_SENTINEL,
                 exclusive=_SENTINEL,
                 durable=_SENTINEL,
                 auto_delete=_SENTINEL,
                 specific_poker=_SENTINEL
                 ):
        """
        This method initializes the queue object.

        :param queue_name: The provided queue name.
        :type queue_name: str

        :param exchange: The provided exchange.
        :type exchange: Exchange

        :param routing_key: The provided routing key.
        :type routing_key: RoutingKey

        :param exclusive: The provided exclusive parameter.
        :type exclusive: bool

        :param auto_delete: The provided auto delete parameter.
        :type auto_delete: bool

        :param specific_poker: The provided specific poker id to handle the object.
        :type specific_poker: pokeman.Pokeman
        """
        from pokeman.amqp_resources.heapq import ResourceHeapQ
        self._exchange_converter = lambda: Exchange(
            specific_poker=self._specific_poker_setter(
                specific_poker=specific_poker,
                lambda_call=True
            )).exchange_name if exchange is self._SENTINEL else exchange.exchange_name
        self._routing_key_converter = lambda: RoutingKey().key if routing_key is self._SENTINEL else routing_key.key
        self.queue_name = self._queue_name_setter(queue_name=queue_name)
        self.exchange = self._exchange_setter(exchange=self._exchange_converter())
        self.routing_key = self._routing_key_setter(routing_key=self._routing_key_converter())
        self.exclusive = self._exclusive_setter(exclusive=exclusive)
        self.durable = self._durable_setter(durable=durable)
        self.auto_delete = self._auto_delete_setter(auto_delete=auto_delete)
        self.specific_poker = self._specific_poker_setter(specific_poker=specific_poker)
        ResourceHeapQ.add_resource(resource=self, specific_poker=self.specific_poker)

    def _queue_name_setter(self, queue_name):
        if queue_name is self._SENTINEL:
            return "pokeman_queue"
        else:
            return queue_name

    def _exchange_setter(self, exchange):
        return exchange

    def _routing_key_setter(self, routing_key):
        return routing_key

    def _exclusive_setter(self, exclusive):
        if exclusive is self._SENTINEL:
            return False
        else:
            return exclusive

    def _durable_setter(self, durable):
        if durable is self._SENTINEL:
            return True
        else:
            return durable

    def _auto_delete_setter(self, auto_delete):
        if auto_delete is self._SENTINEL:
            return False
        else:
            return auto_delete

    def _specific_poker_setter(self, specific_poker, lambda_call=False):
        if specific_poker is self._SENTINEL:
            return None
        elif lambda_call is False:
            return specific_poker.poker_id
        elif lambda_call is True and specific_poker == self.specific_poker:
            return None
        elif lambda_call is True and specific_poker != self.specific_poker:
            return specific_poker
        else:
            raise ValueError('Unresolvable specific poker parameter')
