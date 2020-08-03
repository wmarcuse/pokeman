from abc import ABC, abstractmethod
import inspect
from uuid import uuid4
import atexit
import os

import pokeman.amqp_resources.heapq as _heapq_
from pokeman import coatings
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
        self.poker_id = str(uuid4())
        self.composite = None
        self.connection = None
        self._declared = False
        self.channels = []
        self.cleaned_up = False
        _heapq_.ResourceHeapQ.create_database(poker_id=self.poker_id)
        atexit.register(self.cleanup)
        LOGGER.debug('Initializing Pokeman on current os: {OS} OK!'.format(OS=_current_os))

    @abstractmethod
    def connection_parameters(self, composite):
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
    def connection_parameters(self, composite):
        self.composite = composite

    def start(self):
        """
        This method starts the Pokeman by attaching the connection
        with the AMQP broker to itself.
        """
        if self.composite is not None:
            self.connection = self.composite.connect(poker_id=self.poker_id)
        else:
            raise AttributeError('No connection parameters set for the Pokeman.')

    def stop(self):
        """
        Stop the Pokeman by closing the channels that are attached to the Pokeman
        and the connection.
        """
        LOGGER.debug("Stopping Pokeman")
        self._close_channels()
        self.composite.disconnect(connection=self.connection)
        self.cleanup()
        LOGGER.debug("Stopping Pokeman OK!")

    def _close_channels(self):
        """
        This method closes the channels attached to the Pokeman with AMQP
        broker by sending the Channel.Close RPC command.
        """
        LOGGER.debug("Closing Pokeman channels")
        for channel in self.channels:
            channel.close()
        LOGGER.debug("Closing Pokeman channels OK!")

    def apply_resources(self):
        if self.connection is not None:
            _heapq_.ResourceHeapQ.apply_resources(connection=self.connection, poker_id=self.poker_id)
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
        if self.connection is not None:
            _coating = coating
            _coating.exchange = coating.exchange(_pkid=self)
            self.apply_resources()
            foreman = Foreman()
            foreman.pick_builder(connection=self.connection, coating=_coating, ptype=ptype)
            producer = foreman.deliver_producer()
            self.channels.append(producer.channel)
            return producer
        else:
            raise ConnectionError('No active connection set. Make sure to start the Pokeman first.')

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
        if self.connection is not None:
            _coating = coating
            _coating.exchange = coating.exchange(_pkid=self)
            self.apply_resources()
            foreman = Foreman()
            foreman.pick_builder(connection=self.connection, coating=_coating, ptype=ptype)
            consumer = foreman.deliver_consumer()
            self.channels.append(consumer.channel)
            return consumer
        else:
            raise ConnectionError('No active connection set. Make sure to start the Pokeman first.')

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
        if self.connection is not None:
            resource_manager = ResourceManager(connection=self.connection)
            resource_manager.delete_attached_resources(poker_id=self.poker_id)
        else:
            raise ConnectionError('No active connection set. Make sure to start the Pokeman first.')

    def cleanup(self):
        """
        Atexit and regular called cleanup method for the Pokeman object. Its primary
        function is to delete the HeapQ resources.
        """
        if self.connection is not None:
            if self.cleaned_up is False:
                try:
                    LOGGER.debug('Cleaning up Pokeman {POKER_ID}'.format(POKER_ID=self.poker_id))
                    # _heapq_.ResourceHeapQ.remove_heapq(poker_id=self.poker_id)
                    LOGGER.debug('Cleaning up Pokeman {POKER_ID} OK!'.format(POKER_ID=self.poker_id))
                    self.cleaned_up = True
                except FileNotFoundError:
                    pass
            else:
                pass
        else:
            raise ConnectionError('No active connection set. Make sure to start the Pokeman first.')