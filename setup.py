# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='barbeque',
    version='0.1.0',
    description='Incremental processing framework on BigQuery'
    long_description=readme,
    author='Rendy B. Junior',
    author_email='rendy.b.junior@gmail.com'
    url='https://github.com/rendybjunior/barbeque'
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)
