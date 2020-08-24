from pokeman import Pokeman, ConnectionParameters, Exchange, Queue, RoutingKey
from pokeman.coatings import BasicMessageConsumer, Ptypes

# Always, first declare the Pokeman
# The AMQP resources will be attached to the first (default) Pokeman instance
poker = Pokeman()

# Set the connection parameters
# connection_parameters = BasicConnection(connstr='amqp://guest:guest@localhost:5672')
connection_parameters = ConnectionParameters(connstr='amqp://guest:guest@localhost:5672')

# Apply the connection parameters to the Pokeman
poker.set_parameters(connection=connection_parameters)

# Start poking around, by connecting the Pokeman with your AMQP broker
poker.start()

# Create your AMQP resources objects
# The resources will be assigned to a HeapQ attached to the default Pokeman, or specific Pokeman if declared
my_exchange = Exchange(exchange_name='my_exchange')
my_routing_key = RoutingKey(key='my.routing.key')
my_queue = Queue(queue_name='my_queue', exchange=my_exchange, routing_key=my_routing_key, specific_poker=poker)

# Apply the resources to the AMQP broker, that's it!
# You can verify i.e. via the RabbitMQ Management plugin that the resources are created http://127.0.0.1:15672/
poker.apply_resources()


# Set a callback method
def callback_method(body, properties):
    print('CALLBACK METHOD CALLED')
    print('CORRELATION_ID: {CORRELATION_ID}'.format(CORRELATION_ID=properties.correlation_id))
    print('BODY: {BODY}'.format(BODY=body))
    print('HEADERS: {HEADERS}'.format(HEADERS=properties.headers))


# Set up a basic consumer coating and provide it with the callback method
basic_message_consumer_coating = BasicMessageConsumer(
    exchange=my_exchange,
    queue=my_queue,
    callback_method=callback_method,
    qos=1
)

# Declare the consumer
sync_consumer_1 = poker.declare_consumer(coating=basic_message_consumer_coating, ptype=Ptypes.SYNC_CONSUMER)

# Start consuming
# The consumer will keep looking for new messages, you can optionally limit this with a maximum message count
sync_consumer_1.start(count=2)

# Stop the Pokeman
poker.stop()

