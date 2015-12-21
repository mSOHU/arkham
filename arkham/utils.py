#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 11/3/2015 9:34 AM
"""

import os
import sys
import json
import random
import signal
import decimal
import logging
import datetime
import warnings
import functools
import importlib
import traceback
from exceptions import Warning, StandardError


LOGGER = logging.getLogger(__name__)


def load_entry_point(ep):
    module_name, entry_point = ep.rsplit(':', 1)
    sys.path.append(os.getcwd())
    module = importlib.import_module(module_name)
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


RAND_STRING = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'


def gen_rand_string(length=8):
    return ''.join(random.sample(RAND_STRING * length, length))


class ArkhamWarning(Warning, StandardError):
    @classmethod
    def warn(cls, message):
        warnings.warn(message, cls, stacklevel=3)


def find_config(config_path, entry_point):
    """find config file when running in virtualenv"""
    if config_path.startswith('/'):
        return config_path

    _path = config_path
    if os.path.exists(_path) and os.path.isfile(_path):
        return _path

    _path = os.path.join('etc', config_path)
    if os.path.exists(_path) and os.path.isfile(_path):
        return _path

    module = __import__(entry_point.split('.', 1)[0])
    _path = os.path.join(os.path.dirname(module.__file__), config_path)
    if os.path.exists(_path) and os.path.isfile(_path):
        return _path

    return _path


def handle_term(callback):
    def _handler(_s, frame):
        traceback.print_stack(frame)
        callback()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def init_logging():
    formatter = logging.Formatter(
        '[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)

    logger = logging.getLogger('arkham')
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(handler)


def handle_closed(fn):
    """handle closed connection / channel only once
    """
    @functools.wraps(fn)
    def _wrapper(self, *args, **kwargs):
        try:
            with self.ensure_service():
                return fn(self, *args, **kwargs)
        except self.ConnectionReset:
            with self.ensure_service():
                return fn(self, *args, **kwargs)

    return _wrapper


class SmartJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        elif isinstance(o, datetime.datetime):
            return o.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return super(SmartJsonEncoder, self).default(o)
