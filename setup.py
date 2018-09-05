from setuptools import setup


setup(
    name="harvester",
    version="0.1",
    py_modules=["harvester"],
    install_requires=["Click"],
    entry_points="""
        [console_scripts]
        harvest=harvester:cli
    """,
)
