#!/usr/bin/env python
# coding: utf-8

from setuptools import setup, find_packages


long_description = """
Elizabeth Arkham Asylum for the Criminally Insane
"""

setup(
    name='arkham',
    version='0.0.1',
    description=long_description,
    long_description=long_description,
    author='the S.H.I.E.L.D TEAM',
    author_email='waptech@sohu-inc.com',
    url='http://m.sohu.com',
    packages=find_packages(),
    install_requires=[
        'pyyaml',
        'pika==0.10.0',
    ],
    entry_points={
        'console_scripts': [
            'arc=arkham.consumer:consumer_entry',
            'ark-consumer=arkham.consumer:consumer_entry',
            'ark-rpc=arkham.rpc:rpc_entry',
        ]
    },
)
