from abc import ABC, abstractmethod


class AbstractConfig(ABC):
    """
    Abstract base class for Enterprise Service Bus connection configuration.
    """
    @abstractmethod
    def __init__(
            self,
            connection_attempts,
            heartbeat,
            retry_delay
    ):
        self.CONNECTION_ATTEMPTS = connection_attempts
        self.HEARTBEAT = heartbeat
        self.RETRY_DELAY = retry_delay


class BasicConfig(AbstractConfig):
    """
    Basic configuration for Enterprise Service Bus connection.
    """
    def __init__(
            self,
            connection_attempts=3,
            heartbeat=3600,
            retry_delay=1
    ):
        super().__init__(
            connection_attempts=connection_attempts,
            heartbeat=heartbeat,
            retry_delay=retry_delay
        )
