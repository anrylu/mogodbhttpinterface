import re

from setuptools import setup

with open("mongodbhttpinterface/__init__.py", encoding="utf8") as f:
    version = re.search(r'__version__ = "(.*?)"', f.read()).group(1)

# Metadata goes in setup.cfg. These are here for GitHub's dependency graph.
setup(
    name="MongoDB HTTP Interface",
    version=version,
    install_requires=[
        "pymongo",
        "flask",
    ],
    packages=[
        'mongodbhttpinterface'
    ]
)
