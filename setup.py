#!/usr/bin/env python
from setuptools import find_packages, setup

setup(
    name="tap-yotpo",
    version="2.1.0",
    description="Singer.io tap for extracting data from the Yotpo API",
    author="Stitch",
    url="https://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_yotpo"],
    install_requires=[
        "singer-python==6.8.0",
        "requests==2.34.2",
    ],
    extras_require={
        "dev": [
            "pylint",
            "ipdb",
        ]
    },
    entry_points="""
    [console_scripts]
    tap-yotpo=tap_yotpo:main
    """,
    packages=find_packages(exclude=["tests"]),
    package_data={"schemas": ["tap_yotpo/schemas/*.json"]},
    include_package_data=True,
)
