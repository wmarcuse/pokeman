from enum import Enum


class Ptypes(Enum):

    ASYNC_PRODUCER = 0x10
    SYNC_PRODUCER = 0x20

    ASYNC_CONSUMER = 0x30
    SYNC_CONSUMER = 0x40

    __BasicMessage__ = ~ASYNC_PRODUCER & SYNC_PRODUCER
    __PollingConsumer__ = ASYNC_CONSUMER & ~SYNC_CONSUMER
    __BasicMessageConsumer__ = ~ASYNC_CONSUMER & SYNC_CONSUMER
    __SelectiveConsumer__ = ~ASYNC_CONSUMER & SYNC_CONSUMER

    @classmethod
    def _map(cls, eip, ptype):
        if '__{EIP}__'.format(EIP=eip) not in cls.__dict__.keys():
            raise KeyError('The provided coating has no mapping configuration')

        elif ptype is None or ptype.name not in cls.__dict__.keys():
            raise KeyError('The provided Ptype does not exist')

        elif cls.__dict__['__{EIP}__'.format(EIP=eip)] != ptype.value:
            raise ValueError('The provided coating is not compatible with the provided Ptype.\n'
                             'Check the mapping compatibility with Ptypes.option_check()')

        elif '__{EIP}__'.format(EIP=eip) in cls.__dict__.keys() and ptype.name in cls.__dict__.keys():
            return dict(map=ptype)

        else:
            raise ValueError('The provided coating and Ptype could not be resolved')

    @classmethod
    def option_check(cls, eip):
        if cls.__dict__['__{EIP}__'.format(EIP=eip)] == -0xf1:
            return dict(options=None)

        elif cls.__dict__['__{EIP}__'.format(EIP=eip)] == 0xa0:
            return dict(options=[cls.SYNC_PRODUCER])

        elif cls.__dict__['__{EIP}__'.format(EIP=eip)] == 0x50:
            return dict(options=[cls.ASYNC_PRODUCER])

        elif cls.__dict__['__{EIP}__'.format(EIP=eip)] == 0x0:
            return dict(options=[cls.SYNC_PRODUCER, cls.ASYNC_PRODUCER])
