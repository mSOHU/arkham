#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 10/27/2015 8:19 PM
"""

import inspect
import logging
import argparse

from arkham.service import ArkhamService
from arkham.utils import load_entry_point


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(dest='consumer_name', help='name of consumer service')
    parser.add_argument('-c', '--config', dest='config_path', required=True, help='full path of config.yaml')
    parser.add_argument('-e', '--entry', dest='entry_point', required=True, help='full entry class path')
    return parser.parse_args()


def consumer_entry():
    cmd_args = parse_arguments()

    ArkhamService.init_config(cmd_args.config_path)
    subscriber = ArkhamService.get_instance(cmd_args.consumer_name)
    consumer = load_entry_point(cmd_args.entry_point)

    assert inspect.isclass(consumer), 'consumer must be a class'
    assert issubclass(consumer, ArkhamConsumer), 'consumer class must be subclass of ArkhamService'

    generator = subscriber.consume(
        no_ack=consumer.no_ack,
        inactivity_timeout=consumer.inactivity_timeout
    )

    logger = logging.getLogger('%s.%s' % (consumer.__module__, consumer.__name__))
    inactivate_state = False

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
    inactivity_timeout = None
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
    def consume(cls, message, headers, properties):
        pass

    @classmethod
    def inactivate(cls):
        pass
