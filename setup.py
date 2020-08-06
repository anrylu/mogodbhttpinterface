import re

from setuptools import setup

with open("mongodbhttpinterface/__init__.py") as f:
    version = re.search(r'__version__ = "(.*?)"', f.read()).group(1)

# Metadata goes in setup.cfg. These are here for GitHub's dependency graph.
setup(
    name="mongodbhttpinterface",
    version=version,
    author="Anry Lu",
    author_email="anrylu@qnap.com",
    install_requires=[
        "pymongo",
        "flask",
    ],
    packages=[
        'mongodbhttpinterface'
    ]
)
