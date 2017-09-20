# -* coding: utf-8 -*-

from setuptools import setup

try:
    from pypandoc import convert_file

    long_description = convert_file('README.md', 'md')

except ImportError:
    long_description = """
    分布式计算引擎，将计算打包分发给节点执行，然后在做聚合，适用于上下文无关的计算。
    计算应该容易被拆分
"""

setup(name='lambda2',
      description='简单的分布式计算引擎',
      long_description=long_description,
      version='0.1.1',
      url='https://github.com/xxlv/lambda2',
      author='ghost',
      author_email='lvxiang119@gmail.com',
      license='MIT',
      classifiers=[
          'Intended Audience :: System Administrators',
          'Programming Language :: Python :: 3'
      ],
      packages=['lambda2'],
      install_requires=[
          'PyYAML>=3.11',
          'sh>=1.11'
      ],
      entry_points={
          'console_scripts': [
          ]
      },
      scripts=[
          "bin/lambda2"
      ]
      )
