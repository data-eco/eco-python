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

with open("README.rst") as f:
    readme = f.read()

with open("LICENSE") as f:
    license = f.read()

setup(
    name="iodat",
    version="0.5.0",
    classifiers=CLASSIFIERS,
    description="Io Python Library",
    long_description=readme,
    author="V. Keith Hughitt",
    author_email="keith.hughitt@nih.gov",
    url="https://github.com/io-dat/python",
    license=license,
    scripts=["bin/io"],
    packages=find_packages(exclude=("tests", "docs")),
)
