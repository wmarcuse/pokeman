import sqlite3
import json
import os
from queue import PriorityQueue
import tempfile
from uuid import uuid4

from pokeman.amqp_resources.resolvers import ResourceResolver, Action
from pokeman.amqp_resources.builders import ResourceManager

import logging

LOGGER = logging.getLogger(__name__)


_DB_TYPE = 'file'
_HEAPQ_PREFIX = 'heapq_'
_DB_MODE = 'file'
_DB_CACHE = 'shared'
_MAIN_RESOURCE_TABLE = 'resources'
_POKEMAN_DEFAULT_HEAPQ_ENVIRON = 'POKEMAN_DEFAULT_HEAPQ'


class ResourceHeapQ:
    """
    The ResourceHeapQ managing class, the resources can be managed per process.
    Each Pokeman instance attaches a unique HeapQ to itself, the first to be declared
    will be set default by specifying the process durable environment variable.
    """

    @staticmethod
    def _set_heapq_id(poker_id):
        """
        Static method to set the HeapQ id.

        :param poker_id: The provided poker id.
        :type poker_id: str

        :return: The heapq id.
        :rtype: str
        """
        return '{PREFIX}{POKER_ID}'.format(
            PREFIX=_HEAPQ_PREFIX,
            POKER_ID=poker_id
        )

    @staticmethod
    def _set_heapq_location(heapq_id):
        """
        Static method to set the HeapQ location.

        ..note::
            The location will be set on the Temp
            directory on the Operating System.

        :param heapq_id: The provided heapq id.
        :type heapq_id: str

        :return: The heapq location for the heapq database.
        :rtype: str
        """
        return r'{TEMP_DIR}\{HEAPQ_ID}.heapq'.format(
                TEMP_DIR=tempfile.gettempdir(),
                HEAPQ_ID=heapq_id
            )

    @staticmethod
    def _set_heapq_environment_variable(heapq_id):
        """
        Static method to set the POKEMAN_DEFAULT_HEAPQ environment
        variable for the runtime.

        :param heapq_id: The provided heapq id.
        :type heapq_id: str

        :return: The default status for the provided heapq id.
        :rtype: bool
        """
        if _POKEMAN_DEFAULT_HEAPQ_ENVIRON not in os.environ:
            default_heapq = True
            os.environ[_POKEMAN_DEFAULT_HEAPQ_ENVIRON] = heapq_id
        else:
            default_heapq = False
        return default_heapq

    @staticmethod
    def _heapq_connect(heapq_location):
        """
        Static method to connect to the HeapQ database.

        :param heapq_location: The provided heapq location.
        :type heapq_location: str

        :return: The database connection handler.
        :rtype: sqlite3.Connection
        """
        _connection = sqlite3.connect('{DB_TYPE}:{LOCATION}?cache={CACHE}'.format(
            DB_TYPE=_DB_TYPE,
            LOCATION=heapq_location,
            CACHE=_DB_CACHE
        ), uri=True)
        return _connection

    @classmethod
    def create_database(cls, poker_id):
        """
        Method to create the poker specific HeapQ database in the
        operating system temporary directory. The first poker that is
        instantiated in the current process will set it's HeapQ as default.

        :param poker_id: The provided poker id.
        :type poker_id: str
        """
        heapq_id = cls._set_heapq_id(poker_id=poker_id)
        default_heapq = cls._set_heapq_environment_variable(heapq_id=heapq_id)
        LOGGER.debug('Creating HeapQ database. Default HeapQ: {DEFAULT}'.format(DEFAULT=default_heapq))
        heapq_location = cls._set_heapq_location(heapq_id=heapq_id)
        heapq_connection = cls._heapq_connect(heapq_location=heapq_location)
        cursor = heapq_connection.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS {TABLE}(
            id TEXT PRIMARY KEY, 
            priority TEXT, 
            blueprint TEXT,
            poker_id TEXT,
            status TEXT
            )
        '''.format(TABLE=_MAIN_RESOURCE_TABLE))
        LOGGER.debug('Creating HeapQ database. Default HeapQ: {DEFAULT} OK!'.format(DEFAULT=default_heapq))

    @classmethod
    def add_resource(cls, resource, specific_poker=None):
        """
        This method is invoked by an AMQP resource, passing itself
        as function argument. It can also provide a specific poker
        Pokeman instance to only let that specific poker apply the
        resources to the AMQP broker, while the apply_resources method
        also applies the pending resources in default HeapQ.

        :param resource: The provided resource.
        :type resource: Exchange, Queue

        :param specific_poker: The provided specific poker id.
        :type specific_poker: str
        """
        LOGGER.debug('Adding resource to HeapQ: {RESOURCE}. Specific poker: {POKER_ID}'.format(
            RESOURCE=resource.__class__.__name__,
            POKER_ID=str(specific_poker)
        )
        )
        if specific_poker is None and _POKEMAN_DEFAULT_HEAPQ_ENVIRON in os.environ:
            heapq_save_id = os.environ[_POKEMAN_DEFAULT_HEAPQ_ENVIRON]
            heapq_id = None
        elif specific_poker is not None:
            heapq_save_id = cls._set_heapq_id(poker_id=specific_poker)
            heapq_id = specific_poker
        else:
            raise SyntaxError('No default resource heapq found. Make sure to initialize the '
                              'pokeman first before declaring resources.')
        heapq_db_location = r'{TEMP_DIR}\{HEAPQ_ID}.heapq'.format(
                TEMP_DIR=tempfile.gettempdir(),
                HEAPQ_ID=heapq_save_id
            )
        _resource_database = sqlite3.connect('{DB_TYPE}:{LOCATION}?&cache={CACHE}'.format(
            DB_TYPE=_DB_TYPE,
            LOCATION=heapq_db_location,
            CACHE=_DB_CACHE
        ), uri=True)
        resource_resolver = ResourceResolver()
        template = resource_resolver.resolve(resource=resource, action=Action.CREATE)
        str_template = json.dumps(template)
        priority = template['priority']
        new_status = "PENDING"
        resource_id = str(uuid4())
        cursor = _resource_database.cursor()
        cursor.execute('''INSERT INTO ''' + _MAIN_RESOURCE_TABLE + '''(id, priority, blueprint, poker_id, status)
                  VALUES(?,?,?,?,?)''',
                       (resource_id, priority, str_template, heapq_id, new_status))
        _resource_database.commit()
        LOGGER.debug('Adding resource to HeapQ: {RESOURCE} OK!'.format(RESOURCE=resource.__class__.__name__))

    @classmethod
    def fetch_resources(cls, poker_id):
        """
        This method fetches the poker specific pending resources from
        the HeapQ database and also fetches the pending resources from
        the default HeapQ databse.

        :param poker_id: The provided poker id.
        :type poker_id: str

        :return: The list of pending resources.
        :rtype: list
        """
        LOGGER.debug('Fetching resources from HeapQ')
        if _POKEMAN_DEFAULT_HEAPQ_ENVIRON not in os.environ:
            raise SyntaxError('No default resource heapq found. Make sure to initialize the '
                              'pokeman first before declaring resources.')
        specific_heapq_id = {
            'heapq_id': cls._set_heapq_id(poker_id=poker_id),
            'default_flag': False
        }
        default_heapq_id = {
            'heapq_id': os.environ[_POKEMAN_DEFAULT_HEAPQ_ENVIRON],
            'default_flag': True
        }
        resources_list = []
        if specific_heapq_id['heapq_id'] == default_heapq_id['heapq_id']:
            id_list = [specific_heapq_id, default_heapq_id]
        else:
            id_list = [specific_heapq_id, default_heapq_id]
        for _id in id_list:
            heapq_db_location = r'{TEMP_DIR}\{HEAPQ_ID}.heapq'.format(
                TEMP_DIR=tempfile.gettempdir(),
                HEAPQ_ID=_id['heapq_id']
            )
            _resource_database = sqlite3.connect('{DB_TYPE}:{LOCATION}?cache={CACHE}'.format(
                DB_TYPE=_DB_TYPE,
                LOCATION=heapq_db_location,
                CACHE=_DB_CACHE
            ), uri=True)
            _id['heapq_id'] = _id['heapq_id'].replace('heapq_', '')
            if _id['default_flag'] is True:
                _id['heapq_id'] = None
            select_status = 'PENDING'
            cursor = _resource_database.cursor()
            cursor.execute('''SELECT id, priority, blueprint, poker_id, status FROM ''' + _MAIN_RESOURCE_TABLE
                           + ''' WHERE poker_id IS ? AND status IS ?''', (_id['heapq_id'], select_status))
            rows = cursor.fetchall()
            for row in rows:
                new_status = 'FETCHED'
                cursor.execute(
                    '''UPDATE ''' + _MAIN_RESOURCE_TABLE + ''' SET status = ? WHERE id = ?''',
                    (new_status, row[0]))
                resources_list.append(row)
        LOGGER.debug('Fetching resources from HeapQ OK!')
        return resources_list

    @classmethod
    def get_heapq(cls, poker_id, reverse=False):
        """
        This method sets the HeapQ, by declaring the pending resources
        list as queue.PriorityQueue.

        :param poker_id: The provided poker id.
        :type poker_id: str

        :param reverse: The provided reversed paramater
        :type reverse: bool

        :return: The HeapQ.
        :rtype: queue.PriorityQueue
        """
        LOGGER.debug('Get the HeapQ')
        resources_heapq = PriorityQueue()
        resource_list = cls.fetch_resources(poker_id=poker_id)
        if reverse is False:
            for _resource in resource_list:
                resources_heapq.put((int(_resource[1]), _resource[2]))
        elif reverse is True:
            for _resource in resource_list:
                resources_heapq.put((-int(_resource[1]), _resource[2]))
        LOGGER.debug('Get the HeapQ OK!')
        return resources_heapq

    # TODO: Add FETCHED > CREATED HeapQ update
    @classmethod
    def apply_resources(cls, connection, poker_id):
        """
        This method applies the resources from the HeapQ to the
        AMQP broker. It uses the ResourceManager to select the
        right resource builder and and to create the resource.

        :param connection: The provided connection.
        :type connection: pokeman.Pokeman.connection

        :param poker_id: The provided poker id.
        :type poker_id: str

        .. note::
            The ResourceManager handles the actual creation
            of the resources by using the provided connection.
        """
        LOGGER.debug('Pokeman {POKER_ID} applying resources'.format(POKER_ID=poker_id))
        resource_manager = ResourceManager(connection=connection)
        _heapq = cls.get_heapq(poker_id=poker_id)
        for _ in range(0, _heapq.qsize()):
            heap_record = _heapq.get()
            resource_blueprint = json.loads(heap_record[1])
            resource_manager.pick_builder(blueprint=resource_blueprint)
            resource_manager.create_resource()
        LOGGER.debug('Pokeman {POKER_ID} applying resources OK!'.format(POKER_ID=poker_id))

    @classmethod
    def remove_heapq(cls, poker_id):
        """
        This method removes the HeapQ database for the provided Pokeman id.

        :param poker_id: The provided Pokeman id.
        :type poker_id: str
        """
        LOGGER.debug('Removing HeapQ')
        heapq_id = cls._set_heapq_id(poker_id=poker_id)
        heapq_db_location = r'{TEMP_DIR}\{HEAPQ_ID}.heapq'.format(
            TEMP_DIR=tempfile.gettempdir(),
            HEAPQ_ID=heapq_id
        )
        os.remove(heapq_db_location)
        LOGGER.debug('Removing HeapQ OK!')