#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    "typing_extensions>=3.7",
    'sqlalchemy[mypy]<2',
    "appdirs>=1.4.4 ",
    "SQLAlchemy_Utils>=0.37.8",
    'orjson',
]

test_requirements = [
    "ddt",
]

setup(
    author="Patrick Boettcher",
    author_email='p@yai.se',
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    description="A library which stores, loads and filters time-series-events and catalogues.",
    install_requires=requirements,
    extras_require={
        'dev': [
            'appdirs-stubs',
            "pytest>=4.6.5",
            'pytest',
            'pytest-pep8',
            'pytest-cov',
            'mypy',
            'ddt',
            'pytest-timeout'
        ]
    },
    license="GNU General Public License v3",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='tscat',
    name='tscat',
    packages=find_packages(include=['tscat', 'tscat.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/SciQLop/tscat',
    version='0.2.0',
    zip_safe=False,
)
