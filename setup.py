# -*- coding: utf-8 -*-
import os
from setuptools import setup

if "BW_FUNCTIONAL_VERSION" in os.environ:
    version = os.environ["BW_FUNCTIONAL_VERSION"]
else:
    version = os.environ.get("GIT_DESCRIBE_TAG", "0.0.0")
    if "-" in version:
        versions = version.split("-")
        version = "{}.post{}".format(versions[0], versions[1])

setup(
    version=version,
    packages=["bw_functional"],
    license=open("LICENSE").read(),
    include_package_data=True,
)
