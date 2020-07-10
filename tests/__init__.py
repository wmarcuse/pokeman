import sys
import os
import contextlib
import inspect
import logging
import unittest

LOGGER = logging.getLogger(__name__)


class _Notset:
    def __repr__(self):
        return "<notset>"


notset = _Notset()


class TestAttributes:
    @classmethod
    @contextlib.contextmanager
    def patch(cls, target, name, value=notset):
        """
        Factory method for temporarily monkeypatching objects.

        This method is created because the _pytest.monkeypatch.
        MonkeyPatch() object causes fixture leaking between various
        tests and between files.
        """
        _setattr = []
        if value is notset:
            if not isinstance(value, str):
                raise TypeError(
                    "Use patchfactory(target, name, value) or "
                    "setattr(target, value) with target being a dotted "
                    "import string"
                )
            value = name
            # name, target = derive_importpath(target, raising)

        pre_patched_value = getattr(target, name, notset)
        if pre_patched_value is notset:
            raise AttributeError("{TARGET} has no attribute {NAME}".format(TARGET=target, NAME=name))

        # avoid class descriptors like staticmethod/classmethod
        if inspect.isclass(target):
            pre_patched_value = target.__dict__.get(name, notset)
        _setattr.append((target, name, pre_patched_value))
        setattr(target, name, value)

        try:
            LOGGER.debug("Patch yield for: {TARGET}.{NAME} OK!".format(TARGET=target.__dict__.get(name, notset),
                                                                  NAME=name))
            yield target
        finally:
            for obj, name, value in reversed(_setattr):
                if value is not notset:
                    setattr(obj, name, value)
                else:
                    delattr(obj, name)
            LOGGER.debug("Patch yield for: {TARGET}.{NAME} DESTROYED!".format(TARGET=target.__dict__.get(name, notset),
                                                                       NAME=name))
            _setattr[:] = []

    @classmethod
    @contextlib.contextmanager
    def chdir(cls, path):
        """
        Change the current working directory to the specified path.
        Path can be a string or a py.path.local object.
        """
        pre_scenario_cwd = os.getcwd()
        if hasattr(path, "chdir"):
            path.chdir()
        else:
            os.chdir(path)
        try:
            yield
        finally:
            os.chdir(pre_scenario_cwd)
