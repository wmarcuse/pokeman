from abc import ABCMeta as NativeABCMeta


class DummyAttribute:
    """
    Dummy attribute object.
    """
    pass


def abstract_attribute(obj=None):
    """
    Use this method for setting abstract attributes (properties).

    :param obj: The decorated object.
    :return: The object
    """
    if obj is None:
        obj = DummyAttribute()
    obj.__is_abstract_attribute__ = True
    return obj


class ABCMeta(NativeABCMeta):
    """
    Override the native ABCMeta class.
    """

    def __call__(cls, *args, **kwargs):
        instance = NativeABCMeta.__call__(cls, *args, **kwargs)
        abstract_attributes = {
            name
            for name in dir(instance)
            if getattr(getattr(instance, name), '__is_abstract_attribute__', False)
        }
        if abstract_attributes:
            raise NotImplementedError(
                "Can't instantiate abstract class {} with"
                " abstract attributes: {}".format(
                    cls.__name__,
                    ', '.join(abstract_attributes)
                )
            )
        return instance