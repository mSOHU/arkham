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


class HealthyCheckerMixin(object):
    healthy_check_exchange = 'exchange.healthy'
    healthy_context = None

    @classmethod
    def health_check(cls):
        return {
            'message': 'success',
            'status': 'ok',
            'timestamp': time.time(),
            'data': cls.healthy_context,
        }

    @classmethod
    def ensure_healthy_context(cls):
        if cls.healthy_context:
            return cls.healthy_context

        try:
            import hyperbolic_module
            healthy_context = hyperbolic_module.__dict__.copy()
        except ImportError:
            healthy_context = {}

        healthy_context.update({
            key[len('HEALTHY_'):].lower: value
            for key, value in os.environ.keys()
            if key.startswith('HEALTHY_')
        })

        assert 'instance_id' in healthy_context and 'process_num' in healthy_context, \
            'invalid context, instance_id, process_num required'
        cls.healthy_context = healthy_context
        return cls.healthy_context

    @classmethod
    def _healthy_consumer(cls, channel, method, properties, body):
        try:
            result = cls.health_check()
        except Exception:
            result = {
                'status': 'error',
                'timestamp': time.time(),
                'context': cls.healthy_context,
                'message': traceback.format_exc(),
            }

        try:
            channel.basic_publish('', properties.reply_to, json.dumps(result))
            channel.basic_ack(method.delivery_tag)
        except Exception as err:
            channel.basic_reject(method.delivery_tag)
            cls.logger.exception(
                'Exception occurs when trying to reply healthy check. %r, %r', err, result)

    @classmethod
    def prepare_healthy_check(cls, subscriber):
        context = cls.ensure_healthy_context()

        channel = subscriber.make_channel()

        exchange = cls.healthy_check_exchange
        routing_key = '%s.%s' % (context['instance_id'], context['process_num'])

        queue_name = 'queue.gen-%s' % gen_rand_string(22)
        channel.queue_declare(queue_name, exclusive=True, auto_delete=True)
        channel.queue_bind(queue_name, exchange, routing_key)
        channel.basic_consume(cls._healthy_consumer, queue_name)
