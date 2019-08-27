import os

from setuptools import (
    find_packages,
    setup
)


with open(os.path.join('requirements.txt'), 'r') as f:
    REQUIRED_PACKAGES = f.readlines()


PACKAGES = find_packages()


setup(
    name='sciencebeam-airflow',
    version='0.0.1',
    install_requires=REQUIRED_PACKAGES,
    packages=PACKAGES,
    include_package_data=True,
    description='Apache Airflow pipeline for ScienceBeam'
)
