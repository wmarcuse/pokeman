from pokeman import Pokeman, BasicConnection, Exchange, Queue, RoutingKey
from pokeman.coatings import PollingEndpoint, Ptypes

# Always, first declare the Pokeman
# The AMQP resources will be attached to the first (default) Pokeman instance
poker = Pokeman()

# Set the connection parameters
connection_parameters = BasicConnection(connstr='amqp://guest:guest@localhost:5672')

# Apply the connection parameters to the Pokeman
poker.connection_parameters(composite=connection_parameters)

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


def callback_method(body, headers):
    print('CALLBACK METHOD CALLED')


polling_endpoint_coating = PollingEndpoint(
    exchange=my_exchange,
    queue=my_queue,
    callback_method=callback_method,
    qos=1
)

consumer_1 = poker.declare_consumer(coating=polling_endpoint_coating, ptype=Ptypes.SYNC_CONSUMER)