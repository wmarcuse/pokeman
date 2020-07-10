from pokeman.pokeman import Pokeman
from pokeman.amqp_resources.globals import Exchange, RoutingKey, Queue
from pokeman.composite import BasicConnection, BasicConfig
from pokeman import coatings


__version__ = '0.0.1'


__all__ = [
    # Globals
    '__version__', 'Pokeman',
    'Exchange', 'RoutingKey', 'Queue',

    # Connection
    'BasicConnection', 'BasicConfig',

    # EIP
    'coatings']

import platform


_current_os = platform.system()

