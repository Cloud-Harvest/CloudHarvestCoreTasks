from setuptools import setup, find_packages

# Load requirements from requirements.txt
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

config = dict(name="CloudHarvestCoreTasks",
              version="0.1.0",
              description="This is the Core Task system for CloudHarvest.",
              author="Cloud Harvest, Fiona June Leathers",
              license="CC Attribution-NonCommercial-ShareAlike 4.0 International",
              url="https://github.com/Cloud-Harvest/CloudHarvestCoreTasks",
              packages=find_packages(),
              install_requires=requirements)

setup(**config)
