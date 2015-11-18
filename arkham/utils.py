#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 11/3/2015 9:34 AM
"""

import os
import sys
import random


def load_entry_point(ep):
    module_name, entry_point = ep.rsplit(':', 1)
    sys.path.append(os.getcwd())
    module = __import__(module_name)
    for name in module_name.split('.')[1:] + [entry_point]:
        module = getattr(module, name)

    return module


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
