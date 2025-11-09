from setuptools import setup, find_packages

setup(
    name="queuectl",
    version="1.0.0",
    packages=find_packages(),
    install_requires=["click", "tabulate"],
    entry_points={
        "console_scripts": ["queuectl=src.cli:cli"]
    },
    python_requires=">=3.10",
)
