#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-yotpo",
    version="1.3.0",
    description="Singer.io tap for extracting data from the Yotpo API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_yotpo"],
    install_requires=[
        "singer-python==5.12.1",
        "requests==2.20.0",
        "pendulum==2.1.2",
    ],
    extras_require={
        'dev': [
            'pylint',
            'ipdb',
        ]
    },
    entry_points="""
    [console_scripts]
    tap-yotpo=tap_yotpo:main
    """,
    packages=["tap_yotpo"],
    package_data = {
        "schemas": ["tap_yotpo/schemas/*.json"]
    },
    include_package_data=True,
)
