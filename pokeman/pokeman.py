from abc import ABC, abstractmethod
import inspect
from uuid import uuid4
import atexit
import os

from pokeman.amqp_resources import heapq as _heapq_
from pokeman.composite.connection import Connection, SelectConnection
from pokeman.coatings.ptypes import Ptypes
from pokeman.coatings.builders import Foreman
from pokeman.amqp_resources.builders import ResourceManager

import logging

LOGGER = logging.getLogger(__name__)


class AbstractPokeman(ABC):
    """
    Abstract Pokeman structure.
    """
    def __init__(self):
        """
        This method initializes the Pokeman setting the basic
        configuration parameters, creating the attached HeapQ
        and registering the Atexit method.
        """
        from pokeman import _current_os
        LOGGER.debug('Initializing Pokeman on current os: {OS}'.format(OS=_current_os))
        self.POKER_ID = str(uuid4())
        self.connection_parameters = None
        self.MSCID = 'main_{POKER_ID}'.format(POKER_ID=self.POKER_ID)
        self.connections = {
            'sync': {
                self.MSCID: None
            },
            'async': {}
        }

        self.MSC = lambda: self.connections['sync'][self.MSCID]
        self._declared = False
        self.channels = []
        self.cleaned_up = False
        _heapq_.ResourceHeapQ.create_database(poker_id=self.POKER_ID)
        atexit.register(self.cleanup)
        LOGGER.debug('Initializing Pokeman on current os: {OS} OK!'.format(OS=_current_os))

    @abstractmethod
    def set_parameters(self, connection):
        pass

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def apply_resources(self):
        pass

    @abstractmethod
    def declare_producer(self, coating, type):
        pass

    @abstractmethod
    def declare_consumer(self, coating, type):
        pass

    @abstractmethod
    def declare_router(self):
        pass

    @abstractmethod
    def declare_transformator(self):
        pass

    @abstractmethod
    def declare_channel_manager(self):
        pass

    @abstractmethod
    def declare_system_management_tool(self):
        pass

    @abstractmethod
    def cleanup(self):
        pass


class Pokeman(AbstractPokeman):
    """
    The main Pokeman class that handles all the composite resources by
    distributing tasks to the Foreman and indirect the Wingman.
    """
    def set_parameters(self, connection):
        self.connection_parameters = connection

    def set_sync_connection(self, main=False):
        if main:
            cid = self.MSCID
        else:
            cid = str(uuid4())
        self.connections['sync'][cid] = Connection(parameters=self.connection_parameters)
        return cid

    def set_async_connection(self):
        cid = str(uuid4())
        self.connections['async'][cid] = SelectConnection(parameters=self.connection_parameters)
        return cid

    def start(self):
        """
        This method starts the Pokeman by attaching the connection
        with the AMQP broker to itself.
        """
        if self.connection_parameters is not None:
            self.set_sync_connection(main=True)
            self.MSC().connect()
        else:
            raise AttributeError('No connection parameters set for the Pokeman.')

    def stop(self):
        """
        Stop the Pokeman by closing the channels that are attached to the Pokeman
        and the connection.
        """
        LOGGER.debug("Stopping Pokeman")
        for name, connection in self.connections['sync'].items():
            if connection is not None:
                connection.close_all_channels()
                connection.disconnect()
        for name, connection in self.connections['async'].items():
            if connection is not None:
                connection.close_all_channels()
                connection.disconnect()
        self.cleanup()
        LOGGER.debug("Stopping Pokeman OK!")

    def apply_resources(self):
        if self.MSC() is not None:
            _heapq_.ResourceHeapQ.apply_resources(connection=self.MSC().connection, poker_id=self.POKER_ID)
        else:
            raise ConnectionError('No active connection set. Make sure to start the Pokeman first.')
        
    def _set_coating_ptype_connection(self, ptype):
        """
        This method assigns the connection type depending on the
        synchronous or asynchronous nature of the coating Ptype.

        :param ptype: The provided Ptype
        :type ptype: Ptypes

        :return: The connection object

        .. note::
            Note that:
            * in case of a SYNC_ Ptype a live connection object is passed
            * in case of a ASYNC_ Ptype a not connected connection object
            is passed, because the asynchronous IOloop needs a callback
            waterfall.
            * in case of a ASYNC_ Ptype a new asynchronous connection
            object is created for each resource.
        """
        if self.MSC() is not None:
            if ptype.name.startswith('SYNC_'):
                return self.MSC().connection
            elif ptype.name.startswith('ASYNC_'):
                cid = self.set_async_connection()
                return self.connections['async'][cid]
        else:
            raise ConnectionError('No active connection set. Make sure to start the Pokeman first.')

    def declare_producer(self, coating, ptype):
        """
        This method invokes the building and delivery of a producer
        with the provided configuration managed by the Foreman.

        :param coating: The provided EIP coating
        :type coating: pokeman.coating

        :param ptype: The provided Ptypes
        :type ptype: pokeman.coating.ptypes.Ptypes

        :return: The producer
        """
        _coating = coating
        _coating.exchange = coating.exchange(_pkid=self)
        self.apply_resources()
        foreman = Foreman()
        foreman.pick_builder(
            connection=self._set_coating_ptype_connection(ptype=ptype),
            coating=_coating,
            ptype=ptype
        )
        producer = foreman.deliver_producer()
        if 'SYNC_' in ptype.name:
            self.channels.append(producer.channel)
        return producer

    def declare_consumer(self, coating, ptype):
        """
        This method invokes the building and delivery of a consumer
        with the provided configuration managed by the Foreman.

        :param coating: The provided EIP coating
        :type coating: pokeman.coating

        :param ptype: The provided Ptypes
        :type ptype: pokeman.coating.ptypes.Ptypes

        :return: The producer
        """
        _coating = coating
        _coating.exchange = coating.exchange(_pkid=self)
        self.apply_resources()
        foreman = Foreman()
        foreman.pick_builder(
            connection=self._set_coating_ptype_connection(ptype=ptype),
            coating=_coating,
            ptype=ptype
        )
        consumer = foreman.deliver_consumer()
        if 'SYNC_' in ptype.name:
            self.channels.append(consumer.channel)
        return consumer

    def declare_router(self):
        raise NotImplementedError(
            '{METHOD} method is not implemented yet for {CLASS} object'.format(
                METHOD=inspect.currentframe().f_code.co_name,
                CLASS=self.__class__.__name__
            )
        )

    def declare_transformator(self):
        raise NotImplementedError(
            '{METHOD} method is not implemented yet for {CLASS} object'.format(
                METHOD=inspect.currentframe().f_code.co_name,
                CLASS=self.__class__.__name__
            )
        )

    def declare_channel_manager(self):
        raise NotImplementedError(
            '{METHOD} method is not implemented yet for {CLASS} object'.format(
                METHOD=inspect.currentframe().f_code.co_name,
                CLASS=self.__class__.__name__
            )
        )

    def declare_system_management_tool(self):
        raise NotImplementedError(
            '{METHOD} method is not implemented yet for {CLASS} object'.format(
                METHOD=inspect.currentframe().f_code.co_name,
                CLASS=self.__class__.__name__
            )
        )

    def delete_attached_resources(self):
        """
        This method deletes all the AMQP resources attached to the Pokeman.

        ..note::
            Warning! Use this method with caution, this will delete all
            the resources attached to the pokeman, included the default
            resources if called from the default Pokeman. This method
            will delete resources that are still in use as well, and
            also deletes resources if they are re-declared in other
            Pokeman's as well but still have the same name.
        """
        if self.MSC() is not None:
            resource_manager = ResourceManager(connection=self.MSC().connection)
            resource_manager.delete_attached_resources(poker_id=self.POKER_ID)
        else:
            raise ConnectionError('No active connection set. Make sure to start the Pokeman first.')

    def cleanup(self):
        """
        Atexit and regular called cleanup method for the Pokeman object. Its primary
        function is to delete the HeapQ resources.
        """
        if self.connections is not None:
            if self.cleaned_up is False:
                try:
                    LOGGER.debug('Cleaning up Pokeman {POKER_ID}'.format(POKER_ID=self.POKER_ID))
                    _heapq_.ResourceHeapQ.remove_heapq(poker_id=self.POKER_ID)
                    LOGGER.debug('Cleaning up Pokeman {POKER_ID} OK!'.format(POKER_ID=self.POKER_ID))
                    self.cleaned_up = True
                except FileNotFoundError:
                    pass
            else:
                pass
        else:
            raise ConnectionError('No active connection set. Make sure to start the Pokeman first.')