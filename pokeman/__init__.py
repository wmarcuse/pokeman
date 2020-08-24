from pokeman.pokeman import Pokeman
from pokeman.amqp_resources.globals import Exchange, RoutingKey, Queue
from pokeman import coatings
from pokeman.composite import ConnectionParameters, Connection, BasicConfig
from pokeman.coatings import BasicMessage, BasicMessageConsumer, PollingConsumer, SelectiveConsumer, Ptypes



__version__ = '0.1.13'


__all__ = [
    # Globals
    '__version__', 'Pokeman',
    'Exchange', 'RoutingKey', 'Queue',

    # Connection
    'ConnectionParameters', 'Connection', 'BasicConfig',

    # EIP
    'coatings'
    # 'BasicMessage', 'BasicMessageConsumer', 'PollingConsumer', 'SelectiveConsumer', 'Ptypes'
    ]

import platform


_current_os = platform.system()

