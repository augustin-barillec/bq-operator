from setuptools import setup, find_namespace_packages
from version import version

with open('README.rst') as f:
    README = f.read()

with open('requirements.txt') as f:
    REQUIREMENTS = f.read()

setup(
    name='bq-operator',
    version=version,
    author='Augustin Barillec',
    author_email='augustin.barillec@ysance.com',
    description=(
        'Library for usual operations on a BigQuery dataset.'),
    long_description=README,
    install_requires=REQUIREMENTS,
    packages=find_namespace_packages(include=['bq_operator*']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"]
)
