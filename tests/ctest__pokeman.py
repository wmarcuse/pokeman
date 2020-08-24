import pytest
import unittest
from time import sleep

import logging

LOGGER = logging.getLogger(__name__)


@pytest.fixture(name="poker", scope="function")
def default_composite_poker():
    from pokeman import Pokeman, BasicConnection, BasicConfig
    from pokeman.amqp_resources.heapq import _POKEMAN_DEFAULT_HEAPQ_ENVIRON
    import os

    # Prevent pytest default HeapQ persistence
    if _POKEMAN_DEFAULT_HEAPQ_ENVIRON in os.environ:
        del os.environ[_POKEMAN_DEFAULT_HEAPQ_ENVIRON]

    composite_config = BasicConfig(
        connection_attempts=3,
        heartbeat=3600,
        retry_delay=1
    )
    composite = BasicConnection(
        connstr='amqp://guest:guest@localhost:5672',
        config=composite_config
    )
    poker = Pokeman()
    poker.connection_parameters(composite=composite)

    yield poker
    del poker


def test__pokeman_composite_connection(poker):
    from pika import BlockingConnection

    poker.start()
    assert isinstance(poker.connection, BlockingConnection)
    poker.stop()
    sleep(.05)


def test_basic_message_construction_coating(poker):
    from pokeman import BasicMessage
    from pokeman import Ptypes
    from pokeman import Exchange, RoutingKey, Queue

    # exchange = Exchange(exchange_name="my_exchange", exchange_type="direct")
    # routing_key = RoutingKey(key="my.key")
    # queue = Queue(queue_name="my_queue", exchange=exchange, routing_key=routing_key)
    rk = RoutingKey(key='my_messagekey')
    Queue(routing_key=rk)
    coating = BasicMessage(app_id="new-po", routing_key=rk)
    poker.start()
    producer_1 = poker.declare_producer(coating=coating, ptype=Ptypes.SYNC_PRODUCER)
    poker.apply_resources()
    # for _ in range(0, 1):
    #     producer_1.publish({"message": 123456})
    # for i in range(0, 10):
    #     producer_1.publish(message={"message": 123456})
    # sleep(6)
    poker.delete_attached_resources()
    poker.stop()

# def test_basic_messaging_endpoint_selective_consumer(poker):
#     from pokeman import BasicMessageConsumer
#     from pokeman import Ptypes
#     from pokeman import Exchange, Queue


