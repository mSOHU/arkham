#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 10/21/2015 11:38 AM
"""


import logging
import functools

import yaml
import pika
import pika.exceptions
from pika.adapters.blocking_connection import BlockingChannel

from .utils import merge_service_config


LOGGER = logging.getLogger(__name__)


class ArkhamService(object):
    VALID_ROLES = ('subscribe', 'publish', )
    CONNECTION_FACTORIES = {
        # 'tornado': pika.TornadoConnection,
        'blocking': pika.BlockingConnection,
    }
    REGISTRY = None
    CONFIG = {}
    CONNECTIONS = {}

    service_role = '__unknown__'
    connection = None
    channel = None

    @classmethod
    def init_config(cls, config):
        if isinstance(config, basestring):
            with open(config, 'rb') as fp:
                config = yaml.load(fp)

        assert isinstance(config, dict), 'wrong config format, plz check the example.'
        ArkhamService.CONFIG = merge_service_config(config)

    @classmethod
    def get_instance(cls, service_name):
        base_cls = ArkhamService
        if not base_cls.REGISTRY:
            base_cls.REGISTRY = {klz.service_role: klz for klz in base_cls.__subclasses__()}

        assert service_name in base_cls.CONFIG, 'no proper config for instance: `%s`' % service_name
        conf = base_cls.CONFIG[service_name]

        assert 'service_role' in conf, 'service `%s` not provide service_role field' % service_name
        assert conf['service_role'] in cls.VALID_ROLES, 'role not valid: `%s`' % conf['service_role']

        if cls is not ArkhamService:
            assert conf['service_role'] == cls.service_role, \
                'invalid role, plz use `%s`' % base_cls.REGISTRY[conf['service_role']].__name__

            return cls.build_instance(service_name, conf)
        else:
            return base_cls.REGISTRY[conf['service_role']].build_instance(service_name, conf)

    @classmethod
    def build_instance(cls, service_name, conf):
        return cls(service_name, conf)

    @classmethod
    def make_connection(cls, conf):
        params = {
            'host': conf.get('host', '127.0.0.1'),
            'port': conf.get('port', 5672),
            'virtual_host': conf.get('vhost', '/'),
        }
        params.update(conf.get('extra_params', {}))

        if conf.get('user') and conf.get('passwd'):
            params['credentials'] = pika.PlainCredentials(
                conf['user'], conf['passwd'], erase_on_connect=True
            )

        parameters = pika.ConnectionParameters(**params)
        conn_factory = cls.CONNECTION_FACTORIES[conf.get('type', 'blocking')]
        return conn_factory(parameters, **conf.get('connection_params', {}))

    @classmethod
    def get_connection(cls, name, conf, force_instance=False):
        strategy = conf.get('strategy', 'channel')
        if strategy == 'connection':
            return cls.make_connection(conf)

        if not force_instance and name in cls.CONNECTIONS:
            return cls.CONNECTIONS[name]

        # FIXME: breaks shared connections
        cls.CONNECTIONS[name] = cls.make_connection(conf)
        return cls.CONNECTIONS[name]

    def __init__(self, name, conf):
        self.name = name
        self.conf = conf
        self.connection = self.get_connection(name, conf)
        self.channel = self.connection.channel()

    def make_channel(self):
        """
        :rtype: BlockingChannel
        """
        try:
            channel = self.connection.channel()
        except (KeyError, pika.exceptions.ConnectionClosed):
            LOGGER.info('Connection closed for `%s`', self.name)
            self.connection = self.get_connection(self.name, self.conf, force_instance=True)
            channel = self.connection.channel()

        return channel

    def handle_declarations(self):
        declarations = self.conf.get('declare', {})
        if not declarations:
            return

        ensure_prefix = lambda s, prefix: s if s.startswith(prefix) else prefix + s

        exchange = declarations.get('exchange')
        if exchange:
            declare_args = {
                'exchange': ensure_prefix(exchange['name'], 'exchange.'),
                'exchange_type': exchange.get('type', 'topic'),
                'passive': exchange.get('passive', True),
            }
            declare_args.update(exchange.get('extra_args') or {})
            self.channel.exchange_declare(**declare_args)

        queue = declarations.get('queue')
        if queue:
            declare_args = {
                'queue': ensure_prefix(queue['name'], 'queue.'),
                'passive': queue.get('passive', True),
            }
            declare_args.update(queue.get('extra_args') or {})
            self.channel.queue_declare(**declare_args)

        binds = declarations.get('binds')
        if binds:
            for bind in binds:
                self.channel.queue_bind(**bind)


def handle_closed(fn):
    @functools.wraps(fn)
    def _wrapper(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except (pika.exceptions.ChannelClosed, pika.exceptions.ConnectionClosed):
            self.channel = self.make_channel()

        return fn(self, *args, **kwargs)
    return _wrapper


class PublishService(ArkhamService):
    service_role = 'publish'

    @handle_closed
    def publish(self, body, mandatory=False, immediate=False, routing_key=None, **kwargs):
        """Publish to the channel with the given exchange, routing key and body.
        For more information on basic_publish and what the parameters do, see:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish

        :param body: The message body
        :type body: str or unicode
        :param bool mandatory: The mandatory flag
        :param bool immediate: The immediate flag
        :param routing_key: defaults to config
        :type routing_key: str | unicode
        :param properties:
            content_type=None, content_encoding=None, delivery_mode=None,
            priority=None, correlation_id=None, reply_to=None, expiration=None, message_id=None,
            timestamp=None, type=None, user_id=None, app_id=None, cluster_id=None
            extra kwargs will put into `headers`
        """
        if kwargs:
            properties = pika.BasicProperties(**{
                key: kwargs.pop(key, None) for key in (
                    'content_type', 'content_encoding', 'delivery_mode',
                    'priority', 'correlation_id', 'reply_to', 'expiration', 'message_id',
                    'timestamp', 'type', 'user_id', 'app_id', 'cluster_id'
                )
            })

            properties.headers = kwargs.pop('headers', {})
            if kwargs:
                properties.headers.update(kwargs)
        else:
            properties = None

        return self.channel.basic_publish(
            exchange=self.conf['exchange'],
            routing_key=routing_key or self.conf['routing_key'],
            body=body, properties=properties,
            mandatory=mandatory, immediate=immediate,
        )


class SubscribeService(ArkhamService):
    service_role = 'subscribe'

    def handle_declarations(self):
        super(SubscribeService, self).handle_declarations()

        exchange = self.conf.get('exchange')
        routing_key = self.conf.get('routing_key')

        if exchange and routing_key:
            self.channel.queue_bind(self.conf['queue_name'], exchange, routing_key)

    @handle_closed
    def basic_get(self, no_ack=False):
        """Get a single message from the AMQP broker. Returns a sequence with
        the method frame, message properties, and body.

        :param bool no_ack: Tell the broker to not expect a reply
        :returns: a three-tuple; (None, None, None) if the queue was empty;
            otherwise (method, properties, body); NOTE: body may be None
        :rtype: (None, None, None)|(spec.Basic.GetOk,
                                    spec.BasicProperties,
                                    str or unicode or None)
        """
        return self.channel.basic_get(self.conf['queue_name'], no_ack=no_ack)

    def get_message(self, no_ack=True):
        """Get a single message from the AMQP broker. Returns a sequence with
        the method frame, message properties, and body.

        :param bool no_ack: Tell the broker to not expect a reply
        :returns: a three-tuple; (None, None) if the queue was empty;
            otherwise (method, properties, body); NOTE: body may be None
        :rtype: (None, None)|(delivery_tag, str or unicode or None)
        """
        method, props, payload = self.basic_get(no_ack=no_ack)

        if no_ack:
            return payload
        else:
            return method and method.delivery_tag, payload

    @handle_closed
    def acknowledge(self, delivery_tag, multiple=False):
        """Acknowledge one or more messages. When sent by the client, this
        method acknowledges one or more messages delivered via the Deliver or
        Get-Ok methods. When sent by server, this method acknowledges one or
        more messages published with the Publish method on a channel in
        confirm mode. The acknowledgement can be for a single message or a
        set of messages up to and including a specific message.

        :param int delivery-tag: The server-assigned delivery tag
        :param bool multiple: If set to True, the delivery tag is treated as
                              "up to and including", so that multiple messages
                              can be acknowledged with a single method. If set
                              to False, the delivery tag refers to a single
                              message. If the multiple field is 1, and the
                              delivery tag is zero, this indicates
                              acknowledgement of all outstanding messages.
        """
        return self.channel.basic_ack(delivery_tag=delivery_tag, multiple=multiple)

    @handle_closed
    def reject(self, delivery_tag, multiple=False, requeue=True):
        """This method allows a client to reject one or more incoming messages.
        It can be used to interrupt and cancel large incoming messages, or
        return untreatable messages to their original queue.

        :param int delivery-tag: The server-assigned delivery tag
        :param bool multiple: If set to True, the delivery tag is treated as
                              "up to and including", so that multiple messages
                              can be acknowledged with a single method. If set
                              to False, the delivery tag refers to a single
                              message. If the multiple field is 1, and the
                              delivery tag is zero, this indicates
                              acknowledgement of all outstanding messages.
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.
        """
        return self.channel.basic_nack(delivery_tag=delivery_tag, multiple=multiple, requeue=requeue)

    @handle_closed
    def consume(self, no_ack=False, exclusive=False,
                arguments=None, inactivity_timeout=None):
        """Blocking consumption of a queue instead of via a callback. This
        method is a generator that yields each message as a tuple of method,
        properties, and body. The active generator iterator terminates when the
        consumer is cancelled by client or broker.

        Example:

            for method, properties, body in channel.consume('queue'):
                print body
                channel.basic_ack(method.delivery_tag)

        You should call `BlockingChannel.cancel()` when you escape out of the
        generator loop.

        If you don't cancel this consumer, then next call on the same channel
        to `consume()` with the exact same (queue, no_ack, exclusive) parameters
        will resume the existing consumer generator; however, calling with
        different parameters will result in an exception.

        :param bool no_ack: Tell the broker to not expect a ack/nack response
        :param bool exclusive: Don't allow other consumers on the queue
        :param dict arguments: Custom key/value pair arguments for the consumer
        :param float inactivity_timeout: if a number is given (in
            seconds), will cause the method to yield None after the given period
            of inactivity; this permits for pseudo-regular maintenance
            activities to be carried out by the user while waiting for messages
            to arrive. If None is given (default), then the method blocks until
            the next event arrives. NOTE that timing granularity is limited by
            the timer resolution of the underlying implementation.
            NEW in pika 0.10.0.

        :yields: tuple(spec.Basic.Deliver, spec.BasicProperties, str or unicode)

        :raises ValueError: if consumer-creation parameters don't match those
            of the existing queue consumer generator, if any.
            NEW in pika 0.10.0
        """
        return self.channel.consume(
            self.conf['queue_name'], no_ack=no_ack, exclusive=exclusive,
            arguments=arguments, inactivity_timeout=inactivity_timeout
        )
