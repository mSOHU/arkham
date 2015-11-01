#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 10/27/2015 8:19 PM
"""

import os
import sys
import inspect
import logging
import argparse

import yaml

from arkham.service import ArkhamService


def load_entry_point(ep):
    module_name, entry_point = ep.rsplit(':', 1)
    sys.path.append(os.getcwd())
    module = __import__(module_name)
    return getattr(module, entry_point)


def merge_service_config(config):
    config = config.copy()

    def _merge_dict(_d, _u):
        for key, value in _u.items():
            _d.setdefault(key, value)

    global_conf = config.pop('global', {})
    services_conf = {}
    for name, service in config.items():
        services_conf[name] = service
        _merge_dict(services_conf[name], global_conf)

    return services_conf


def consumer_entry():
    parser = argparse.ArgumentParser()
    parser.add_argument(dest='consumer_name', help='name of consumer service')
    parser.add_argument('-c', '--config', dest='config_path', required=True, help='full path of config.yaml')
    parser.add_argument('-e', '--entry', dest='entry_point', required=True, help='full entry class path')
    cmd_args = parser.parse_args()

    with open(cmd_args.config_path, 'rb') as fp:
        config = yaml.load(fp)

    ArkhamService.init_config(merge_service_config(config))

    subscriber = ArkhamService.get_instance(cmd_args.consumer_name)
    consumer = load_entry_point(cmd_args.entry_point)

    assert inspect.isclass(consumer), 'consumer must be a class'
    assert issubclass(consumer, ArkhamConsumer), 'consumer class must be subclass of ArkhamService'

    generator = subscriber.consume(
        no_ack=consumer.no_ack,
        inactivity_timeout=consumer.inactivate_timeout
    )

    logger = logging.getLogger('%s.%s' % (consumer.__module__, consumer.__name__))

    for method, properties, body in generator:
        if not method:
            # inactivate notice
            try:
                consumer.inactivate()
            except Exception as err:
                logger.exception('Exception occurs in inactivate handler: %r' % err)

            continue

        try:
            consumer.consume(body, headers=properties.headers, properties=properties)
        except consumer.suppress_exceptions as err:
            logger.exception('Message rejected due exception: %r' % err)
            if not consumer.no_ack:
                subscriber.reject(method.delivery_tag)
        else:
            if not consumer.no_ack:
                subscriber.acknowledge(method.delivery_tag)


class ArkhamConsumer(object):
    no_ack = False
    suppress_exceptions = ()

    # int / float. if set, will call ArkhamConsumer.inactivate when timed-out
    inactivate_timeout = None

    @classmethod
    def get_service(cls, service_name):
        return ArkhamService.get_instance(service_name)

    @classmethod
    def consume(cls, message, headers, properties):
        pass

    @classmethod
    def inactivate(cls):
        pass
