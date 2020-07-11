Pokeman
=======

|Version| |Status| |Python versions| |Build status| |Coverage| |License|


Pokeman is a RabbitMQ (AMQP 0-9-1) service choreography library for Python, implementing
various instantly deployable service resources like producers and consumers.

The service resources are inspired by the `Enterprise Integration Patterns <https://www.enterpriseintegrationpatterns.com/patterns/messaging/>`_.


Introduction
------------
Pokeman is a service choreography library for Python, currently built on top of `Pika <https://github.com/pika/pika>`_.

- Manage RabbitMQ resources like Connections, Channels, Exchanges and Queues with OOP style objects.
- Deploy multiple producers and consumers with just a few lines of code.
- Use Pokeman at multiple Python-based (micro-)services to implement easy service choreography.


# Run tests

    docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

.. |Version| image:: https://img.shields.io/pypi/v/pokeman
   :target: https://pypi.org/project/pokeman

.. |Status| image:: https://img.shields.io/pypi/status/pokeman
  :target: https://github.com/wmarcuse/pokeman

.. |Python versions| image:: https://img.shields.io/pypi/pyversions/pokeman
    :target: https://pypi.org/project/pokeman

.. |Build Status| image:: https://api.travis-ci.org/wmarcuse/pokeman.png?branch=master
  :target: https://travis-ci.org/github/wmarcuse/pokeman

.. |Coverage| image:: https://codecov.io/gh/wmarcuse/pokeman/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/wmarcuse/pokeman

.. |License| image:: https://img.shields.io/github/license/wmarcuse/pokeman
  :target: https://github.com/wmarcuse/pokeman