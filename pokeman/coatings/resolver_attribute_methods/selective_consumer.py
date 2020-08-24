import json

import logging

LOGGER = logging.getLogger(__name__)


def start(self):
    self.start_consuming()


def on_message(self, channel, method, properties, body):
    """
    Invoked by pika when a message is delivered from the AMQP broker. The
    channel is passed for convenience. The basic_deliver object that
    is passed in carries the exchange, routing key, delivery tag and
    a redelivered flag for the message. The properties passed in is an
    instance of BasicProperties with the message properties and the body
    is the message that was sent.

    :param channel: The channel object.
    :type channel: pika.channel.Channel

    :param method: basic_deliver method.
    :type method: pika.Spec.Basic.Deliver

    :param properties: The properties.
    :type properties: pika.Spec.BasicProperties

    :param body: The message body.
    :type body: bytes
    """
    try:
        print('message received')
        print(properties.correlation_id)
        if properties.correlation_id == self.correlation_id_reference:
            print("SUCCEEDEEDRT")
            self.callback_method(json.loads(body), properties)
            self.acknowledge_message(method.delivery_tag)
            self.channel.stop_consuming()
    except Exception:
        LOGGER.exception("Synchronous callback method exception:")