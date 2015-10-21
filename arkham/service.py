#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'johnxu'
__date__ = '10/21/2015 11:38 AM'


import logging
import functools

import yaml
import pika
import pika.exceptions
from pika.adapters.blocking_connection import BlockingChannel


LOGGER = logging.getLogger(__name__)


class ArkhamService(object):
    VALID_ROLES = ('broadcast', 'subscribe', 'publish', )
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

        ArkhamService.CONFIG = config

    @classmethod
    def get_instance(cls, service_name):
        base_cls = ArkhamService
        if not base_cls.REGISTRY:
            base_cls.REGISTRY = {klz.service_role: klz for klz in base_cls.__subclasses__()}

        assert service_name in base_cls.CONFIG, 'no proper config for instance: `%s`' % service_name
        conf = base_cls.CONFIG[service_name]

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
        self.initialize()

    def initialize(self):
        pass

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

    def initialize(self):
        declare_args = self.conf.get('exchange_declare_args', {})
        if declare_args:
            declare_args['exchange'] = self.conf['exchange']
            declare_args['exchange_type'] = self.conf['exchange_type']
            self.channel.exchange_declare(**declare_args or {})

    @handle_closed
    def publish(self, body, properties=None, mandatory=False, immediate=False, routing_key=None):
        """Publish to the channel with the given exchange, routing key and body.
        For more information on basic_publish and what the parameters do, see:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish

        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param body: The message body
        :type body: str or unicode
        :param pika.spec.BasicProperties properties: Basic.properties
        :param bool mandatory: The mandatory flag
        :param bool immediate: The immediate flag
        """
        return self.channel.basic_publish(
            exchange=self.conf['exchange'],
            routing_key=routing_key or self.conf['bind_key'],
            body=body, properties=properties,
            mandatory=mandatory, immediate=immediate,
        )


class SubscribeService(ArkhamService):
    service_role = 'subscribe'
    channel = None

    def initialize(self):
        declare_args = self.conf.get('queue_declare_args', {})
        if declare_args:
            if 'queue_name' in declare_args:
                declare_args['queue'] = self.conf['queue_name']

            method = self.channel.queue_declare(**declare_args or {})
            self.conf['queue_name'] = method.method.queue
        else:
            self.channel.queue_declare(self.conf['queue_name'], passive=True)

        if self.conf.get('bind_key'):
            self.channel.queue_bind(self.conf['queue'], self.conf['exchange'], self.conf['bind_key'])

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

    def get_message(self, no_ack=False):
        method, props, payload = self.basic_get(no_ack=no_ack)
        return payload

    @handle_closed
    def consume(self, no_ack=False, exclusive=False,
                arguments=None, inactivity_timeout=None):
        return self.channel.consume(
            self.conf['queue_name'], no_ack=no_ack, exclusive=exclusive,
            arguments=arguments, inactivity_timeout=inactivity_timeout
        )
