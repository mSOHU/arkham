#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'johnxu'
__date__ = '10/21/2015 11:38 AM'


import sys
import logging

import pika
import pika.exceptions
from pika.adapters.blocking_connection import BlockingChannel
import yaml


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

    role_name = '__unknown__'

    @classmethod
    def init_config(cls, config):
        if isinstance(config, basestring):
            with open(config, 'rb') as fp:
                config = yaml.load(fp)

        cls.CONFIG = config

    @classmethod
    def get_instance(cls, service_name):
        if not cls.REGISTRY:
            cls.REGISTRY = {klz.service_role: klz for klz in cls.__subclasses__()}

        assert service_name in cls.CONFIG, 'no proper config for instance: `%s`' % service_name
        conf = cls.CONFIG[service_name]

        assert conf['role'] == cls.role_name, 'invalid role, plz use `%s`' % cls.REGISTRY[conf['role']].__name__
        return cls.build_instance(service_name, conf)

    @classmethod
    def build_instance(cls, service_name, conf):
        return cls(service_name, conf)

    @classmethod
    def make_connection(cls, conf):
        params = {
            'host': conf.get('host', '127.0.0.1'),
            'port': conf.get('host', 5672),
            'virtual_host': conf.get('vhost', '/'),
        }
        params.update(conf.get('extra_params', {}))

        if conf.get('user') and conf.get('passwd'):
            params['credentials'] = pika.PlainCredentials(
                conf['user'], conf['passwd'], erase_on_connect=True
            )

        parameters = pika.ConnectionParameters(**params)
        conn_factory = cls.CONNECTION_FACTORIES[conf.get('type', 'blocking')]
        return conn_factory(parameters, **conf.get('connection_params'))

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

    def make_channel(self, no_context=False):
        """
        :rtype: BlockingChannel
        """
        try:
            channel = self.connection.channel()
        except (KeyError, pika.exceptions.ConnectionClosed):
            LOGGER.info('connection closed for `%s`', self.name)
            self.connection = self.get_connection(self.name, self.conf, force_instance=True)
            channel = self.connection.channel()

        if no_context:
            return channel

        return ChannelContext(channel)


class ChannelContext(object):
    def __init__(self, channel_obj):
        """
        :type channel_obj: BlockingChannel
        """
        self.channel_obj = channel_obj

    def __enter__(self):
        return self.channel_obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.channel_obj.close()


class PublishService(ArkhamService):
    role_name = 'publish'

    def initialize(self):
        declare_args = self.conf.get('exchange_declare_args', {})
        if declare_args:
            declare_args['exchange'] = self.conf['exchange']
            declare_args['exchange_type'] = self.conf['exchange_type']
            with self.make_channel() as channel:
                channel.exchange_declare(**declare_args or {})

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
        with self.make_channel() as channel:
            return channel.basic_publish(
                exchange=self.conf['exchange'],
                routing_key=routing_key or self.conf['bind'],
                body=body, properties=properties,
                mandatory=mandatory, immediate=immediate,
            )


def handle_closed_channel(times=3):
    def _decorator(fn):
        def _wrapper(self, *args, **kwargs):
            i = 0
            exc_info = None, None, None
            while i < times:
                try:
                    return fn(*args, **kwargs)
                except pika.exceptions.ChannelClosed:
                    i += 1
                    exc_info = sys.exc_info()
                    self.channel = self.make_channel(no_context=True)

            raise exc_info[0], exc_info[1], exc_info[2]

        return _wrapper
    return _decorator


class SubscribeService(ArkhamService):
    role_name = 'subscribe'
    channel = None

    def initialize(self):
        self.channel = channel = self.make_channel(no_context=True)

        declare_args = self.conf.get('queue_declare_args', {})
        if declare_args:
            if 'queue_name' in declare_args:
                declare_args['queue'] = self.conf['queue_name']

            method = channel.queue_declare(**declare_args or {})
            self.conf['queue_name'] = method.method.queue
            channel.queue_bind(self.conf['queue'], self.conf['exchange'], self.conf['bind'])

    @handle_closed_channel(times=3)
    def get_message(self, no_ack=False):
        return self.channel.basic_get(no_ack=no_ack)

    def consume(self, no_ack=False, exclusive=False,
                arguments=None, inactivity_timeout=None):
        return self.channel.consume(
            self.conf['queue_name'], no_ack=no_ack, exclusive=exclusive,
            arguments=arguments, inactivity_timeout=inactivity_timeout
        )
