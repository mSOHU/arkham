#!/usr/bin/env python
# coding: utf-8

from setuptools import setup, find_packages


long_description = """
Elizabeth Arkham Asylum for the Criminally Insane
"""

setup(
    name='arkham',
    version='___version___',
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
)
