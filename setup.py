#!/bin/python
from setuptools import setup
import pathlib

HERE = pathlib.Path(__file__).parent

README = (HERE / "README.md").read_text()

setup(name='wellping_ema_parser',
      version='1.0.1',
      description='JSON to CSV converter for Stanford Communities Project',
      long_description=README,
      long_description_content_type="text/markdown",
      url='https://github.com/StanfordSocialNeuroscienceLab/wellping-ema-package',
      author='Ian Richard Ferguson',
      author_email='IRF229@nyu.edu',
      license="MIT",
      classifiers=[
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.7"
      ],
      packages=[
            'scp_ema',
            'scp_ema/parser'
            ],
      include_package_data=True,
      install_requires=[
            'numpy', 'pandas', 'tqdm'
            ]
      )
