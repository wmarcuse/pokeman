import re
import sys
from setuptools import setup, find_packages

with open('pokeman/__init__.py', 'r') as f:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        f.read(), re.MULTILINE).group(1)

with open('README.rst', 'r') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='pokeman',
    version=version,
    description='Pokeman RabbitMQ Service Choreography Library',
    long_description=long_description,
    maintainer='Wolf Marcuse',
    maintainer_email='wmarcuse@gmail.com',
    url='https://github.com/wmarcuse/pokeman',
    packages=find_packages(include=['pokeman']),
    license='BSD',
    install_requires=required,
    package_data={'': ['LICENSE', 'README.rst']},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Communications', 'Topic :: Internet',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Networking'
    ],
    zip_safe=True)