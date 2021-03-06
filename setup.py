from setuptools import find_packages, setup

# Package metadata
NAME = "harvester"
VERSION = "0.1.0"
REQUIRED = [
    "appdirs",
    "bs4",
    "Click",
    "logzero",
    "peewee",
    "pid",
    "requests",
    "tenacity",
    "toml",
]  # Required packages

setup(
    name=NAME,
    version=VERSION,
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=REQUIRED,
    entry_points="""
        [console_scripts]
        harvest=harvest:cli
    """,
)
