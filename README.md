Pokeman
=======

[![Build Status](https://api.travis-ci.org/wmarcuse/pokeman.png?branch=master)](https://api.travis-ci.org/wmarcuse/pokeman)

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