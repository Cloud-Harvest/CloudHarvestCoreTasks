from setuptools import setup, find_packages
from CloudHarvestCoreTasks.meta import meta

# Load requirements from requirements.txt
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

config = dict(packages=find_packages(),
              install_requires=requirements)

config = config | meta


def main():
    setup(**config)


if __name__ == '__main__':
    main()
