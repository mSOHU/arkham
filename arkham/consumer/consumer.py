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
import contextlib

from arkham.service import ArkhamService
from arkham.healthy import HealthyCheckerMixin, HealthyChecker
from arkham.utils import handle_term


LOGGER = logging.getLogger(__name__)


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


def apply_period_callback(connection, callback, args, logger):
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
        connection.add_timeout(timeout, _wrapper)

    _start_timeout = 0 if args['startup_call'] else args['interval']
    last_schedule = [time.time() + _start_timeout]
    connection.add_timeout(_start_timeout, _wrapper)


class ArkhamConsumerRunner(object):
    MAX_SLEEP_TIME = 15

    # we use this to jump out consume() loop
    INACTIVITY_TIMEOUT = 0.5

    def __init__(self, worker_cls, consumer, config_path, consumer_name):
        self.consumer = consumer
        self.logger = self.consumer.logger = self.consumer.logger or LOGGER
        self.logger.setLevel(getattr(logging, self.consumer.log_level))

        # setup flags
        self.inactivate_state = False
        self.stop_flag = False
        self.last_slept = 0
        self.last_activity = time.time()

        # for IDE
        self.generator = []

        self.logger.info('Using %s worker: %r', self.consumer.worker_class, worker_cls)
        self.worker = worker_cls(self)

        ArkhamService.init_config(config_path)
        self.subscriber = ArkhamService.get_instance(consumer_name)

        self.setup_signal_handler()
        if self.consumer.enable_healthy_checker:
            self.setup_healthy_checker()

        self.callbacks = collect_period_callbacks(self.consumer)

    def setup_healthy_checker(self):
        try:
            HealthyChecker(self.subscriber, self.consumer).prepare_healthy_check()
        except AssertionError as _err:
            self.logger.warning('Error preparing healthy checker: %s', _err.message)

    def setup_signal_handler(self):
        def _term_handler():
            self.stop_flag = True
            if not self.worker.is_running():
                self.logger.warning('SIGTERM received. Exiting...')
            else:
                self.logger.warning(
                    'SIGTERM received while processing a message, '
                    'consumer exit is scheduled.')
        handle_term(_term_handler)

    def setup_consumer(self):
        def _on_connect():
            conn = self.subscriber.connection
            for callback, args in self.callbacks.values():
                apply_period_callback(conn, callback, args, self.logger)

            if self.consumer.prefetch_count is not None:
                assert self.consumer.prefetch_count <= 65535, \
                    '`prefetch_count`: %s is larger than limit(65535).' % self.consumer.prefetch_count
                self.subscriber.channel.basic_qos(prefetch_count=self.consumer.prefetch_count)
            self.generator = self.subscriber.consume(
                no_ack=self.consumer.no_ack,
                inactivity_timeout=self.INACTIVITY_TIMEOUT
            )
        self.subscriber.add_connect_callback(_on_connect)

    @contextlib.contextmanager
    def work_context(self, method):
        try:
            yield
        except self.consumer.suppress_exceptions as err:
            self.logger.exception('Exception `%r` suppressed while processing message.', err)
        except Exception as err:
            self.logger.exception('Message rejected due exception: %r', err)
            if not self.consumer.no_ack:
                self.subscriber.reject(method.delivery_tag, requeue=self.consumer.requeue_on_exception)
            return
        finally:
            self.last_activity = time.time()

        if not self.consumer.no_ack:
            self.subscriber.acknowledge(method.delivery_tag)

    def start(self):
        self.setup_consumer()

        loop_counter = 0
        while not self.stop_flag:
            loop_counter += 1

            if loop_counter % 1000 == 0:
                self.logger.debug('Consumer loop counter #%u', loop_counter)

            # fetch message
            try:
                with self.subscriber.ensure_service():
                    try:
                        yielded = next(self.generator)
                    except StopIteration:
                        if not self.stop_flag:
                            #  consumer cancel notification
                            self.logger.warning('Consumer been canceled. Trying to re-consume...')
                            self.generator = self.subscriber.consume(
                                no_ack=self.consumer.no_ack,
                                inactivity_timeout=self.INACTIVITY_TIMEOUT
                            )
                        continue
                    else:
                        # reset sleep time
                        self.last_slept = 0
            except ArkhamService.ConnectionReset:
                if not self.stop_flag:
                    self.last_slept = min(self.MAX_SLEEP_TIME, self.last_slept * 2)
                    self.logger.error('Cannot reach rabbit server, sleep %s second.', self.last_slept)
                    time.sleep(self.last_slept)
                continue

            # inactivate notice
            if not yielded:
                if self.inactivate_state or self.worker.is_running():
                    continue

                if self.consumer.inactivity_timeout is None:
                    # this means we should check the stop_flag
                    continue

                if time.time() - self.last_activity < self.consumer.inactivity_timeout:
                    continue

                try:
                    self.consumer.inactivate()
                    # make sure inactivate handler will be called successfully
                    self.inactivate_state = True
                except Exception as err:
                    self.logger.exception('Exception occurs in inactivate handler: %r' % err)

                continue

            # if yielded is not None, reset inactivate_state flag
            self.inactivate_state = False

            # and spawn worker
            method, properties, body = yielded
            if properties.content_type == 'application/json' and isinstance(body, str):
                body = json.loads(body, encoding='utf8')

            self.worker.spawn(method, properties, body)

        # before exit
        try:
            self.subscriber.channel.cancel()
        finally:
            self.worker.join()
            self.logger.info('Consumer exiting...')


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
    # if exception raised already listed in `suppress_exceptions`
    # message will be ack-ed, exception will only be printed
    # else message will be reject
    suppress_exceptions = ()

    # if exception cannot be ignored, the `requeue_on_exception` indicates
    # whether message will be rejected w/ or w/o requeue-ing
    requeue_on_exception = False

    # int / float. if set, will call ArkhamConsumer.inactivate when timed-out
    inactivity_timeout = None
    logger = None
    log_level = 'WARNING'
    heartbeat_interval = None
    prefetch_count = None

    # service instance cache
    service_instances = {}

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
