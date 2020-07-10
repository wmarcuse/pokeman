# import pytest
#
# from pokeman import Queue, Exchange
#
# import os
# import time
#
#
# @pytest.fixture(name="poker", scope="function")
# def default_composite_poker():
#     from pokeman._pokeman import Pokeman
#     from pokeman import composite
#
#     composite_config = composite.BasicConfig(
#         connection_attempts=3,
#         heartbeat=3600,
#         retry_delay=1
#     )
#     enterprise_service_bus = composite.BlockingESB(
#         connstr='amqp://guest:guest@localhost:5672',
#         config=composite_config
#     )
#     poker = Pokeman()
#     poker.connection_parameters(composite=enterprise_service_bus)
#
#     yield poker
#
#
# def test_heapq(poker):
#
#     from pokeman._pokeman import Pokeman
#     from pokeman import composite
#
#     composite_config = composite.BasicConfig(
#         connection_attempts=3,
#         heartbeat=3600,
#         retry_delay=1
#     )
#     enterprise_service_bus = composite.BlockingESB(
#         connstr='amqp://guest:guest@localhost:5672',
#         config=composite_config
#     )
#     poker_a = Pokeman()
#     print(poker_a.poker_id)
#     poker_a.connection_parameters(composite=enterprise_service_bus)
#     poker_b = Pokeman()
#     print(poker_b.poker_id)
#     poker_b.connection_parameters(composite=enterprise_service_bus)
#
#     exchange_a = Exchange(exchange_name='aaedsadx')
#     exchange_b = Exchange(exchange_name='bbex')
#     Queue(queue_name='aadssaaa', durable=False, auto_delete=True, specific_poker=poker_a, exchange=exchange_a)
#     Queue(queue_name='bbsdsdabb', durable=False, auto_delete=True, specific_poker=poker_b, exchange=exchange_b)
#     exchange_c = Exchange(exchange_name='aatdasdron', durable=False)
#     poker_c = Pokeman()
#     exchange_d = Exchange(exchange_name='cctasdron', durable=False, specific_poker=poker_c)
#     poker_c.connection_parameters(composite=enterprise_service_bus)
#
#     poker_a.start()
#     poker_b.start()
#     poker_a.apply_resources()
#     poker_b.apply_resources()
#     poker_c.start()
#     poker_c.apply_resources()
#
#     poker_c.stop()
#     poker_a.stop()
#     poker_b.stop()
#
