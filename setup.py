#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-lightspeedretail",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_lightspeedretail"],
    install_requires=[
        "singer-python>=5.0.0",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-lightspeedretail=tap_lightspeedretail:main
    """,
    packages=["tap_lightspeedretail"],
    package_data = {
        "schemas": ["tap_lightspeedretail/schemas/*.json"]
    },
    include_package_data=True,
)
