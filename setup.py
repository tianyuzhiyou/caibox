# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

with open('./requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='pyreBox',
    version='0.0.2',
    description='基于redis的封装工具',
    author='caiwanpeng',
    packages=find_packages(),
    install_requires=required,
)