from pokeman.coatings.messaging_endpoints.basic_message_consumer import BasicMessageConsumer


class SelectiveConsumer(BasicMessageConsumer):
    """
    Basic Message Enterprise Integration Pattern for messaging
    endpoint > Selective Consumer.

    A Sentinel is declared default for various pattern parameters,
    as the possible None declaration is a posteriori.
    """
    _SENTINEL = Ellipsis

    def __init__(
            self,
            exchange=_SENTINEL,
            queue=_SENTINEL,
            callback_method=_SENTINEL,
            qos=_SENTINEL,
            correlation_id=_SENTINEL
    ):
        super().__init__(
            exchange=exchange,
            queue=queue,
            callback_method=callback_method,
            qos=qos
        )
        self.correlation_id = self._correlation_id_setter(correlation_id=correlation_id)

    def _correlation_id_setter(self, correlation_id):
        if correlation_id is self._SENTINEL:
            raise ValueError('The SelectiveConsumer needs a selective reference: correlation_id')
        else:
            return correlation_id
