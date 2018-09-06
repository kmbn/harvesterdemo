from setuptools import setup


setup(
    name="harvester",
    version="0.1",
    py_modules=["harvest"],
    install_requires=["Click"],
    entry_points="""
        [console_scripts]
        harvest=harvest:cli
    """,
)
