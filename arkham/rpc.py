#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 11/12/2015 11:43 AM
"""

import json
import time
import logging
import argparse
import traceback

import pika

from arkham.service import ArkhamService
from arkham.utils import load_entry_point


LOGGER = logging.getLogger(__name__)


class TimeoutError(Exception):
    pass


class ServiceNotRegistered(Exception):
    def __init__(self, service_name):
        self.service_name = service_name


class RPCException(Exception):
    def __init__(self, service_name, traceback_text):
        self.service_name = service_name
        self.traceback_text = traceback_text

    def __repr__(self):
        return 'Service `%s` fails: \n%s' % (self.service_name, self.traceback_text)

    __str__ = __repr__


class RPCService(ArkhamService):
    service_role = 'rpc'

    RPC_CONTENT_TYPE = 'application/arkham-rpc'
    RPC_CONTENT_ENCODING = 'json.UTF-8'
    DIRECT_QUEUE = 'amq.rabbitmq.reply-to'
    DIRECT_PROPS = pika.BasicProperties(
        reply_to=DIRECT_QUEUE,
        content_type=RPC_CONTENT_TYPE,
        content_encoding=RPC_CONTENT_ENCODING,
    )

    def call(self, service_name, *args, **kwargs):
        full_result = kwargs.pop('full_result', False)
        routing_key = kwargs.pop('routing_key', None)
        timeout = kwargs.pop('timeout', None)

        channel = self.make_channel()
        result = [None]

        def _callback(ch, method, properties, body):
            if result[0] is not None:
                LOGGER.error('Duplicate result received!')

            if full_result:
                result[0] = (method, properties, body)
            else:
                result[0] = body

            channel.basic_cancel(consumer_tag)

        consumer_tag = channel.basic_consume(_callback, self.DIRECT_QUEUE, no_ack=True)
        package = {
            'service_name': service_name,
            'args': args,
            'kwargs': kwargs,
        }
        channel.basic_publish(
            exchange=self.conf['exchange'],
            routing_key=routing_key or self.conf['routing_key'],
            body=json.dumps(package, ensure_ascii=False),
            properties=self.DIRECT_PROPS,
        )

        start_time = time.time()
        while result[0] is None:
            if timeout is not None and (time.time() - start_time) > timeout:
                raise TimeoutError()

            self.connection.process_data_events(0.01)

        if full_result:
            return result[0]

        reply = json.loads(result[0])
        if reply['status'] == 'fail':
            raise RPCException(service_name, reply['message'])
        else:
            return reply['data']


class ArkhamRPCServer(object):
    def __init__(self):
        self.registry = {}

    def service(self, service_name):
        def _decorator(fn):
            self.registry[service_name] = fn
            return fn
        return _decorator

    def handle_message(self, message):
        service_fn = self.registry.get(message['service_name'])
        if service_fn is None:
            raise ServiceNotRegistered(message['service_name'])

        return service_fn(*message['args'], **message['kwargs'])

    def parse_message(self, method, message, properties):
        assert properties.content_type == RPCService.RPC_CONTENT_TYPE, 'invalid content-type'
        assert properties.content_encoding == RPCService.RPC_CONTENT_ENCODING, 'unsupported content-encoding'
        return json.loads(message)

    def start(self, consumer_name):
        subscriber = ArkhamService.get_instance(consumer_name)
        for method, properties, body in subscriber.consume():
            try:
                message = self.parse_message(method, body, properties)
            except (ValueError, TypeError, AssertionError) as err:
                LOGGER.error('Invalid request in RPC queue. [%s], %r', err.message, properties.__dict__)
                subscriber.reject(method.delivery_tag, requeue=False)
                continue

            try:
                result = self.handle_message(message)
                reply = {
                    'status': 'ok',
                    'message': 'success',
                    'data': result,
                }
            except Exception:
                reply = {
                    'status': 'fail',
                    'message': traceback.format_exc(),
                    'data': None,
                }

            try:
                subscriber.channel.basic_publish('', properties.reply_to, json.dumps(reply))
                subscriber.acknowledge(method.delivery_tag)
            except Exception as err:
                subscriber.reject(method.delivery_tag)
                LOGGER.exception('Exception occurs when trying to reply RPC. %r, %r', err, reply)


default_server = ArkhamRPCServer()
service = default_server.service


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(dest='consumer_name', help='name of consumer service')
    parser.add_argument('-c', '--config', dest='config_path', required=True, help='full path of config.yaml')
    parser.add_argument('-e', '--entry', dest='entry_point', required=True, help='full entry class path')
    return parser.parse_args()


def rpc_entry():
    cmd_args = parse_arguments()

    ArkhamService.init_config(cmd_args.config_path)
    server = load_entry_point(cmd_args.entry_point)

    assert isinstance(server, ArkhamRPCServer), 'consumer class must be subclass of ArkhamRPCServer'
    server.start(cmd_args.consumer_name)
