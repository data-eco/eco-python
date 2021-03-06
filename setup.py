# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Science/Research",
    "License :: OSI Approved :: BSD License",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Topic :: Scientific/Engineering",
    "Operating System :: Microsoft :: Windows",
    "Operating System :: POSIX",
    "Operating System :: Unix",
    "Operating System :: MacOS",
]

with open("README.md") as f:
    readme = f.read()

with open("LICENSE") as f:
    license = f.read()

setup(
    name="eco",
    version="0.6.0",
    classifiers=CLASSIFIERS,
    description="eco Python library",
    long_description=readme,
    author="V. Keith Hughitt",
    author_email="keith.hughitt@gmail.com",
    url="https://github.com/khughitt/eco-python",
    license=license,
    scripts=["bin/eco"],
    packages=find_packages(exclude=("tests", "docs")),
)
