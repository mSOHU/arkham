#!/usr/bin/env python
# coding: utf-8

import re
from setuptools import setup, find_packages


description = """
Elizabeth Arkham Asylum for the Criminally Insane
"""

# Get the version
version_regex = r'__version__ = ["\']([^"\']*)["\']'
with open('arkham/__init__.py', 'r') as f:
    text = f.read()
    match = re.search(version_regex, text)

    if match:
        version = match.group(1)
    else:
        raise RuntimeError("No version number found!")


setup(
    name='arkham',
    version=version,
    description=description,
    long_description=description,
    author='the S.H.I.E.L.D TEAM',
    author_email='waptech@sohu-inc.com',
    url='https://github.com/mSOHU/arkham',
    packages=find_packages(),
    install_requires=[
        'pyyaml',
        'pika==0.10.1.dev1',
        'gevent>=1.1.0',
    ],
    entry_points={
        'console_scripts': [
            'arc=arkham.consumer.entry:consumer_entry',
            'ark-consumer=arkham.consumer.entry:consumer_entry',
            'ark-rpc=arkham.rpc:rpc_entry',
        ]
    },
)
