from setuptools import find_packages, setup

setup(
    name="demo_integration",
    packages=find_packages(),
    install_requires=[
        "dagster",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
