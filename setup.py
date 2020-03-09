"""Installation setup of the frbcat python package."""
from setuptools import setup

setup(name='frbcat',
      version='0.1.5',
      description='Query the FRB catalogue',
      url='http://github.com/davidgardenier/frbcat',
      author='David Gardenier',
      author_email='davidgardenier@gmail.com',
      license='MIT',
      packages=['frbcat'],
      install_requires=['pandas', 'requests', 'numpy'],
      zip_safe=False)
