#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 5/25/2016 8:51 PM
"""

import inspect
import argparse

from arkham.utils import (
    ArkhamWarning, load_entry_point, find_config
)
from arkham.consumer.worker import WORKER_CLASSES


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(dest='consumer_name', help='name of consumer service')
    parser.add_argument('-c', '--config', dest='config_path',
                        required=True, help='full path of config.yaml')
    parser.add_argument('-e', '--entry', dest='entry_point',
                        required=True, help='full entry class path')
    parser.add_argument('-k', '--worker', dest='worker_type',
                        required=True, default='sync',
                        help='worker type: <gevent|sync>')
    return parser.parse_args()


def consumer_entry():
    cmd_args = parse_arguments()
    worker_type = cmd_args.worker_type

    assert worker_type in WORKER_CLASSES, \
        'Unsupported worker type: `%s`' % worker_type

    worker_cls = WORKER_CLASSES[worker_type]
    worker_cls.patch()

    consumer = load_entry_point(cmd_args.entry_point)

    assert inspect.isclass(consumer), 'consumer must be a class'

    from arkham.consumer.consumer import ArkhamConsumer, ArkhamConsumerRunner
    assert issubclass(consumer, ArkhamConsumer), 'consumer class must be subclass of ArkhamConsumer'

    has_kwargs = bool(inspect.getargspec(consumer.consume.im_func).keywords)
    if not has_kwargs:
        ArkhamWarning.warn('consume function should have **kwargs.')

    runner = ArkhamConsumerRunner(
        worker_cls, consumer,
        find_config(cmd_args.config_path, cmd_args.entry_point),
        cmd_args.consumer_name
    )
    runner.start()
