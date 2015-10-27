#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 10/27/2015 8:19 PM
"""

import sys
import inspect

import yaml

from . import SubscribeService


USAGE_STRING = """usage:
arc consumer.yaml

example configuration:

server:
  host:
  port:
  vhost:
  user:
  passwd:

consumer:
  entry: arkham.consumer:Consumer
  queue_name:
  # optional if queue is bound to exchange already
  exchange: exchange_name
  routing_key: routing_key
"""


def load_entry_point(ep):
    module_name, entry_point = ep.rsplit(':', 1)
    module = __import__(module_name)
    return getattr(module, entry_point)


def consumer_entry():
    if len(sys.argv) != 2:
        print USAGE_STRING
        return

    config_path = sys.argv[1]
    with open(config_path, 'rb') as fp:
        config = yaml.load(fp)

    entry_point = config['consumer'].pop('entry')

    service_config = {}
    service_config.update(config['server'])
    service_config.update(config['consumer'])

    SubscribeService.init_config({
        '__consumer': entry_point
    })

    subscriber = SubscribeService.get_instance('__consumer')
    consumer = load_entry_point(entry_point)

    assert inspect.isclass(consumer), 'consumer must be a class'
    assert issubclass(consumer, ArkhamConsumer), 'consumer class must be subclass of ArkhamService'

    for method, properties, body in subscriber.consume(no_ack=consumer.no_ack):
        try:
            consumer.consume(body, headers=properties.headers, properties=properties)
        except consumer.reject_exceptions:
            if not consumer.no_ack:
                subscriber.reject(method.delivery_tag)
        except consumer.suppress_exceptions:
            continue
        else:
            subscriber.acknowledge(method.delivery_tag)


class ArkhamConsumer(object):
    no_ack = False
    suppress_exceptions = ()
    reject_exceptions = ()

    @classmethod
    def consume(cls, message, headers, properties):
        pass
