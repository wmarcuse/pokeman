from pokeman import Pokeman, BasicConnection, Exchange, RoutingKey, Queue


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

# Remove AMQP resources attached to the Pokeman
poker.delete_attached_resources()




