# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

with open('./requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='cwp-caibox',
    version='0.0.1',
    description='cwp的编程工具箱',
    author='caiwanpeng',
    packages=find_packages(),
    install_requires=required,
)