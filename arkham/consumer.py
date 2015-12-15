#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 10/27/2015 8:19 PM
"""

import json
import time
import inspect
import logging
import argparse

from arkham.service import ArkhamService
from arkham.healthy import HealthyCheckerMixin, HealthyChecker
from arkham.utils import load_entry_point, ArkhamWarning, find_config, handle_term


LOGGER = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(dest='consumer_name', help='name of consumer service')
    parser.add_argument('-c', '--config', dest='config_path', required=True, help='full path of config.yaml')
    parser.add_argument('-e', '--entry', dest='entry_point', required=True, help='full entry class path')
    return parser.parse_args()


def collect_period_callbacks(consumer):
    if consumer.heartbeat_interval:
        period_callback(consumer.heartbeat_interval)(consumer.heartbeat.im_func)

    callbacks = {}
    for attr_name in dir(consumer):
        attr = getattr(consumer, attr_name)
        if not inspect.ismethod(attr) or not hasattr(attr.im_func, 'periodically_args'):
            continue

        callbacks[attr_name] = attr, attr.im_func.periodically_args

    return callbacks


def apply_period_callback(ioloop, callback, args, logger):
    def _wrapper():
        try:
            callback()
        except Exception as err:
            logger.exception('Exception occurs in callback %s: %r' % (callback.__name__, err))

        now_time = time.time()
        next_schedule = args['interval'] - (now_time - last_schedule[0])
        if args['ignore_tick']:
            # if misses, schedule at next tick
            timeout = next_schedule % args['interval']
        else:
            # if misses, schedule now
            timeout = max(next_schedule, 0)

        last_schedule[0] = now_time + timeout
        ioloop.add_timeout(timeout, _wrapper)

    _start_timeout = 0 if args['startup_call'] else args['interval']
    last_schedule = [time.time() + _start_timeout]
    ioloop.add_timeout(_start_timeout, _wrapper)


def consumer_entry():
    cmd_args = parse_arguments()

    ArkhamService.init_config(find_config(cmd_args.config_path, cmd_args.entry_point))
    subscriber = ArkhamService.get_instance(cmd_args.consumer_name)
    consumer = load_entry_point(cmd_args.entry_point)

    assert inspect.isclass(consumer), 'consumer must be a class'
    assert issubclass(consumer, ArkhamConsumer), 'consumer class must be subclass of ArkhamService'
    has_kwargs = bool(inspect.getargspec(consumer.consume.im_func).keywords)
    if not has_kwargs:
        ArkhamWarning.warn('consume function should have **kwargs.')

    logger = consumer.logger = consumer.logger or LOGGER

    callbacks = collect_period_callbacks(consumer)
    for callback, args in callbacks.values():
        apply_period_callback(subscriber.connection._impl, callback, args, logger)

    try:
        HealthyChecker(subscriber, consumer).prepare_healthy_check()
    except AssertionError as err:
        logger.warning('Error preparing healthy checker: %s', err.message)

    generator = subscriber.consume(
        no_ack=consumer.no_ack,
        inactivity_timeout=consumer.inactivity_timeout
    )

    inactivate_state = False
    stop_flag = [False]
    consuming_flag = False

    def _term_handler():
        if consuming_flag:
            raise KeyboardInterrupt()

        logger.warning('SIGTERM received while processing a message, consumer exit is scheduled.')
        stop_flag[0] = True
    handle_term(_term_handler)

    for yielded in generator:
        # inactivate notice
        if not yielded:
            if inactivate_state:
                continue

            try:
                consumer.inactivate()
                # make sure inactivate handler will be called successfully
                inactivate_state = True
            except Exception as err:
                logger.exception('Exception occurs in inactivate handler: %r' % err)

            continue

        # if yielded is not None, reset inactivate_state flag
        inactivate_state = False
        method, properties, body = yielded

        if properties.content_type == 'application/json' and isinstance(body, str):
            body = json.loads(body, encoding='utf8')

        try:
            consuming_flag = True
            if has_kwargs:
                consumer.consume(body, headers=properties.headers or {}, properties=properties, method=method)
            else:
                consumer.consume(body, headers=properties.headers or {}, properties=properties)
        except consumer.suppress_exceptions as err:
            logger.exception('Message rejected due exception: %r' % err)
            if not consumer.no_ack:
                subscriber.reject(method.delivery_tag)
        else:
            if not consumer.no_ack:
                subscriber.acknowledge(method.delivery_tag)

        consuming_flag = False

        if stop_flag[0]:
            logger.warning('Exiting due SIGTERM.')
            break


def period_callback(interval, startup_call=False, ignore_tick=False):
    """
    :param ignore_tick:
        bool, if True, ignore missed ticks, otherwise, when miss occurs, re-schedule callbacks ASAP
    """
    def _decorator(fn):
        _interval = int(interval)
        assert _interval > 0, 'invalid interval value: %r' % interval
        fn.periodically_args = {
            'interval': _interval,
            'startup_call': startup_call,
            'ignore_tick': ignore_tick,
        }
        return fn
    return _decorator


class ArkhamConsumer(HealthyCheckerMixin):
    no_ack = False
    suppress_exceptions = ()

    # int / float. if set, will call ArkhamConsumer.inactivate when timed-out
    inactivity_timeout = None
    service_instances = {}
    logger = None
    heartbeat_interval = None

    @classmethod
    def get_service(cls, service_name, force=False):
        if force:
            return ArkhamService.get_instance(service_name)

        instance = cls.service_instances.get(service_name)
        if not instance:
            instance = cls.service_instances[service_name] = ArkhamService.get_instance(service_name)

        return instance

    @classmethod
    def consume(cls, message, **kwargs):
        """
        :param kwargs: includes
            - properties
            - headers
            - method
        """
        pass

    @classmethod
    def heartbeat(cls):
        pass

    @classmethod
    def inactivate(cls):
        pass
