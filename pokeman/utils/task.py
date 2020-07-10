from enum import Enum


class TaskStatus(Enum):
    """
    The available task status variables.
    """

    PENDING = 'PENDING'
    SUCCESS = 'SUCCESS'
    ON_HOLD = 'ON_HOLD'
    FAILED = 'FAILED'
