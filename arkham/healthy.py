#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 12/6/2015 8:48 PM
"""

import os
import json
import time
import traceback

from .utils import gen_rand_string
from .rpc import RPCService
from .service import ArkhamService


class HealthyCheckerMixin(object):
    @classmethod
    def health_check(cls):
        return {}


class HealthyChecker(object):
    healthy_check_exchange = 'exchange.healthy'

    def __init__(self, subscriber, consumer_cls):
        assert issubclass(consumer_cls, HealthyCheckerMixin)
        self.consumer_cls = consumer_cls
        self.subscriber = subscriber
        self.healthy_context = self.build_healthy_context()
        self.routing_key = '%s.%s' % (self.healthy_context['instance_id'], self.healthy_context['process_num'])

    @classmethod
    def build_healthy_context(cls):
        try:
            import hyperbolic_module
            healthy_context = hyperbolic_module.__dict__.copy()
        except ImportError:
            healthy_context = {}

        healthy_context.update({
            key[len('SUPER_'):].lower(): value
            for key, value in os.environ.items()
            if key.startswith('SUPER_')
        })

        assert 'instance_id' in healthy_context and 'process_num' in healthy_context, \
            'invalid context, instance_id, process_num required'
        return healthy_context

    def healthy_consumer(self, channel, method, properties, body):
        try:
            payload = self.consumer_cls.health_check()
            result = {
                'status': 'ok',
                'message': 'success',
                'data': {
                    'payload': payload,
                    'timestamp': time.time(),
                    'context': self.healthy_context,
                }
            }
        except Exception:
            result = {
                'status': 'fail',
                'message': traceback.format_exc(),
                'data': {
                    'payload': None,
                    'timestamp': time.time(),
                    'context': self.healthy_context,
                },
            }

        try:
            channel.basic_publish('', properties.reply_to, json.dumps(result))
            channel.basic_ack(method.delivery_tag)
        except Exception as err:
            channel.basic_reject(method.delivery_tag)
            self.consumer_cls.logger.exception(
                'Exception occurs when trying to reply healthy check. %r, %r', err, result)

    def prepare_healthy_check(self):
        channel = self.subscriber.make_channel()
        queue_name = 'queue.gen-%s' % gen_rand_string(22)
        channel.queue_declare(queue_name, exclusive=True, auto_delete=True)
        channel.queue_bind(queue_name, self.healthy_check_exchange, self.routing_key)
        channel.basic_consume(self.healthy_consumer, queue_name)


class HealthyService(RPCService):
    service_role = 'healthy'
    healthy_config = {
        'exchange': HealthyChecker.healthy_check_exchange,
        'service_role': service_role,
    }

    @classmethod
    def get_instance(cls, service_name):
        ArkhamService.CONFIG['__healthy__'] = cls.healthy_config
        return ArkhamService.get_instance(service_name)

    def check(self, routing_key):
        return self.call('', routing_key=routing_key)
