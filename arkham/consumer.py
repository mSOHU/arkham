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
import contextlib

from arkham.service import ArkhamService
from arkham.healthy import HealthyCheckerMixin, HealthyChecker
from arkham.utils import load_entry_point, ArkhamWarning, find_config, handle_term


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


class BaseWorker(object):
    def __init__(self, runner):
        self.runner = runner
        self.consumer = runner.consumer
        self.initialize()

    def initialize(self):
        pass

    def spawn(self, method, properties, body):
        raise NotImplementedError()

    def is_running(self):
        raise NotImplementedError()

    def join(self):
        raise NotImplementedError()


class GeventWorker(BaseWorker):
    pool = None

    def initialize(self):
        import gevent.monkey
        gevent.monkey.patch_all()

        import gevent.pool
        self.pool = gevent.pool.Pool(self.consumer.prefetch_count)

    def spawn(self, method, properties, body):
        def _wrapper():
            with self.runner.work_context(method):
                self.consumer.consume(body, headers=properties.headers or {}, properties=properties, method=method)

        self.pool.spawn(_wrapper)
        self.pool.wait_available()

    def is_running(self):
        return bool(len(self.pool))

    def join(self):
        return self.pool.join()


class SyncWorker(BaseWorker):
    def spawn(self, method, properties, body):
        with self.runner.work_context(method):
            self.consumer.consume(body, headers=properties.headers or {}, properties=properties, method=method)

    def is_running(self):
        return False

    def join(self):
        return


WORKER_CLASSES = {
    'gevent': GeventWorker,
    'sync': SyncWorker,
}


class ArkhamConsumerRunner(object):
    def __init__(self, consumer, config_path, consumer_name):
        self.consumer = consumer
        self.logger = self.consumer.logger = self.consumer.logger or LOGGER

        # setup flags
        self.inactivate_state = False
        self.stop_flag = False

        # for IDE
        self.generator = []

        worker_class = WORKER_CLASSES[self.consumer.worker_class]
        self.logger.info('Using %s worker: %r', self.consumer.worker_class, worker_class)
        self.worker = worker_class(self)

        ArkhamService.init_config(config_path)
        self.subscriber = ArkhamService.get_instance(consumer_name)

        handle_term(self._term_handler)
        self.setup_healthy_checker()

        self.callbacks = collect_period_callbacks(self.consumer)

    def setup_healthy_checker(self):
        try:
            HealthyChecker(self.subscriber, self.consumer).prepare_healthy_check()
        except AssertionError as _err:
            self.logger.warning('Error preparing healthy checker: %s', _err.message)

    def _term_handler(self):
        if not self.worker.is_running():
            self.logger.warning('SIGTERM received. Exiting...')
            ioloop = self.subscriber.connection._impl

            def closer():
                ioloop.close(reply_text='User requested exit due signal SIGTERM')

            ioloop.add_timeout(0, closer)
        else:
            self.logger.warning('SIGTERM received while processing a message, consumer exit is scheduled.')
        self.stop_flag = True

    def setup_consumer(self):
        def _on_connect():
            ioloop = self.subscriber.connection._impl
            for callback, args in self.callbacks.values():
                apply_period_callback(ioloop, callback, args, self.logger)

            self.subscriber.channel.basic_qos(prefetch_count=self.consumer.prefetch_count)
            self.generator = self.subscriber.consume(
                no_ack=self.consumer.no_ack,
                inactivity_timeout=self.consumer.inactivity_timeout
            )
        self.subscriber.add_connect_callback(_on_connect)

    @contextlib.contextmanager
    def work_context(self, method):
        try:
            yield
        except self.consumer.suppress_exceptions as err:
            self.logger.exception('Message rejected due exception: %r' % err)
            if not self.consumer.no_ack:
                self.subscriber.reject(method.delivery_tag)
        else:
            if not self.consumer.no_ack:
                self.subscriber.acknowledge(method.delivery_tag)

    def start(self):
        self.setup_consumer()

        while not self.stop_flag:
            # fetch message
            try:
                with self.subscriber.ensure_service():
                    yielded = next(self.generator)
            except ArkhamService.ConnectionReset:
                if not self.stop_flag:
                    LOGGER.error('Cannot connect to rabbit server, sleep 1 sec...')
                    time.sleep(1)
                continue

            # inactivate notice
            if not yielded:
                if self.inactivate_state:
                    continue

                try:
                    self.consumer.inactivate()
                    # make sure inactivate handler will be called successfully
                    inactivate_state = True
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
        self.worker.join()


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


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(dest='consumer_name', help='name of consumer service')
    parser.add_argument('-c', '--config', dest='config_path', required=True, help='full path of config.yaml')
    parser.add_argument('-e', '--entry', dest='entry_point', required=True, help='full entry class path')
    return parser.parse_args()


def consumer_entry():
    cmd_args = parse_arguments()
    consumer = load_entry_point(cmd_args.entry_point)

    assert inspect.isclass(consumer), 'consumer must be a class'
    assert issubclass(consumer, ArkhamConsumer), 'consumer class must be subclass of ArkhamConsumer'
    has_kwargs = bool(inspect.getargspec(consumer.consume.im_func).keywords)
    if not has_kwargs:
        ArkhamWarning.warn('consume function should have **kwargs.')

    runner = ArkhamConsumerRunner(
        consumer,
        find_config(cmd_args.config_path, cmd_args.entry_point),
        cmd_args.consumer_name
    )
    runner.start()


class ArkhamConsumer(HealthyCheckerMixin):
    no_ack = False
    suppress_exceptions = ()

    # int / float. if set, will call ArkhamConsumer.inactivate when timed-out
    inactivity_timeout = None
    service_instances = {}
    logger = None
    heartbeat_interval = None
    prefetch_count = 0

    # 'sync' or 'gevent'
    # will spawn greenlet for consume, pool size will be `prefetch_count`
    worker_class = 'sync'

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
