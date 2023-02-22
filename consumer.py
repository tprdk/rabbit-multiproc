# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import json
import logging
import time
from multiprocessing import Process, Event, Queue

from pika.exchange_type import ExchangeType
import traceback
import pika

from ulogger import LOG_FORMAT


LOGGER = logging.getLogger(__name__)


class PikaConsumer(object):
    EXCHANGE = 'custom-ex'
    EXCHANGE_TYPE = ExchangeType.topic
    QUEUE = 'custom-q'
    ROUTING_KEY = 'custom-q'

    def __init__(self, amqp_url: str, job_q: Queue):
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self._consuming = False
        self._prefetch_count = 1
        self.job_q = job_q

    def connect(self):
        LOGGER.info('Connecting to %s', self._url)
        return pika.SelectConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            LOGGER.info('Connection is closing or already closed')
        else:
            LOGGER.info('Closing connection')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        LOGGER.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        LOGGER.error('Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reconnect necessary: %s', reason)
            self.reconnect()

    def reconnect(self):
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self.close_connection()

    def setup_exchange(self, exchange_name):
        LOGGER.info('Declaring exchange: %s', exchange_name)
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE,
            callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        LOGGER.info('Exchange declared: %s', userdata)
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        LOGGER.info('Declaring queue %s', queue_name)
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(
            queue=queue_name,
            arguments={"x-max-priority": 20, "x-message-ttl": 60000},
            callback=cb
        )

    def on_queue_declareok(self, _unused_frame, userdata):
        queue_name = userdata
        LOGGER.info('Binding %s to %s with %s', self.EXCHANGE, queue_name,
                    self.ROUTING_KEY)
        cb = functools.partial(self.on_bindok, userdata=queue_name)
        self._channel.queue_bind(
            queue_name,
            self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            callback=cb)

    def on_bindok(self, _unused_frame, userdata):
        LOGGER.info('Queue bound: %s', userdata)
        self.set_qos()

    def set_qos(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        LOGGER.info('QOS set to: %d', self._prefetch_count)
        self.start_consuming()

    def start_consuming(self):
        LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self.QUEUE, self.on_message)
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        LOGGER.debug('Received message # %s: %s',
                     basic_deliver.delivery_tag, body)
        try:
            receive = json.loads(body)
            LOGGER.info(f"Consumed message : {receive}")
            self.job_q.put(receive)
        except Exception as e:
            traceback.print_exc()
            LOGGER.error(e)

        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        self._consuming = False
        LOGGER.info(
            'RabbitMQ acknowledged the cancellation of the consumer: %s',
            userdata)
        self.close_channel()

    def close_channel(self):
        LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        if not self._closing:
            self._closing = True
            LOGGER.info('Stopping')
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            LOGGER.info('Stopped')


class RabbitConsumer(Process):
    """This is an example consumer that will reconnect if the nested
    ExampleConsumer indicates that a reconnect is necessary.

    """

    def __init__(self,
                 amqp_url: str,
                 shutdown_event: Event,
                 job_q: Queue):
        super().__init__()
        self._reconnect_delay = 0
        self._amqp_url = amqp_url
        self._consumer: PikaConsumer = None

        self.shutdown_event: Event = shutdown_event
        self.job_q = job_q


    def init(self):
        self._consumer = PikaConsumer(amqp_url=self._amqp_url, job_q=self.job_q)

    def run(self):
        self.init()

        while not self.shutdown_event.is_set():
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                LOGGER.info("Exiting now.")
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info('Reconnecting after %d seconds', reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer = PikaConsumer(self._amqp_url)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    amqp_url = 'amqp://user:bitnami@localhost:5672'

    job_q = Queue()
    shutdown = Event()
    consumer = RabbitConsumer(amqp_url, shutdown_event=shutdown, job_q=job_q)
    consumer.start()


if __name__ == '__main__':
    main()
