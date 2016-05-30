#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 5/25/2016 8:51 PM
"""

import time
import logging

from arkham.utils import ArkhamWarning


LOGGER = logging.getLogger(__name__)


class BaseWorker(object):
    def __init__(self, runner):
        self.runner = runner
        self.consumer = runner.consumer
        self.logger = runner.consumer.logger
        self.initialize()

    @classmethod
    def patch(cls):
        pass

    def initialize(self):
        pass

    def spawn(self, method, properties, body):
        raise NotImplementedError()

    def is_running(self):
        raise NotImplementedError()

    def join(self):
        raise NotImplementedError()


class GeventWorker(BaseWorker):
    DEFAULT_POOL_SIZE = 20

    pool = None
    loop_threshold = 0.1
    sleep_interval = 0.01

    def loop_watcher(self):
        while True:
            start_time = time.time()
            time.sleep(self.sleep_interval)
            loop_cost = time.time() - start_time - self.sleep_interval

            if loop_cost > self.loop_threshold:
                self.logger.warning(
                    'Gevent loop time cost `%.2fms` > %sms, current pool_size: %u',
                    loop_cost * 1000, self.loop_threshold * 1000, len(self.pool)
                )

    @classmethod
    def patch(cls):
        import gevent.monkey
        gevent.monkey.patch_all()

        # using select.poll in gevent context may cause 100% cpu usage
        import select
        gevent.monkey.remove_item(select, 'poll')

    def initialize(self):
        import gevent.pool
        pool_size = self.consumer.prefetch_count
        if pool_size is None:
            ArkhamWarning.warn('worker_class set to `gevent` but prefetch_count is None, '
                               'pool_size set to %s' % self.DEFAULT_POOL_SIZE)
            pool_size = self.DEFAULT_POOL_SIZE

        self.pool = gevent.pool.Pool(pool_size)

        import gevent
        gevent.spawn(self.loop_watcher).start()

    def spawn(self, method, properties, body):
        def _wrapper():
            with self.runner.work_context(method):
                self.consumer.consume(body, headers=properties.headers or {}, properties=properties, method=method)

        self.pool.spawn(_wrapper)
        self.logger.debug('Gevent pool_size: %s', len(self.pool))

    def is_running(self):
        return bool(len(self.pool))

    def join(self):
        self.logger.info('Joining worker pool, current pool_size: %s', len(self.pool))
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
