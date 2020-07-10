# # docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
#
# def nonex():
#     pass
#
# # class TestUnits(unittest.TestCase):
# def test_async():
#     import pokeman as pm
#
#     async_config = pokeman.composite.config.AsyncConsumerTopicExchangeConfig(topic="search_engine", event="request")
#     async_consumer = pm.ReconnectingAsyncConsumer(connstr='amqp://guest:guest@localhost:5672',
#                                                   config=async_config)
#     async_consumer.run(func=nonex)
#
# test_async()
#
# # TODO: Bij verlies verbinding async consumer werkt de boel niet meer, en blijven messages hangen.