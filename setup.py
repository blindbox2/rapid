from setuptools import setup, find_packages
import os
from typing import List


def read_requirements(filename) -> List[str]:
    with open(filename, 'r') as file:
        requirements = file.read().splitlines()
    return requirements


requirements = read_requirements(os.path.join(os.path.dirname(__file__), 'requirements.txt'))


setup(
    name='boilerplate',
    version='0.1.0',
    packages=find_packages(),
    install_requires=requirements,
)
