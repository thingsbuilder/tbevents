from setuptools import setup, find_packages

setup(name='tbevents',
      version='2.0',
      url='https://github.com/thingsbuilder/tbevents',
      license='GPL',
      author='Cassio R Eskelsen',
      author_email='eskelsen@gmail.com',
      description='This library provides functionality for an event-driven architecture using brokers like RabbitMQ as Event Bus',
      packages=find_packages(),
      long_description=open('README.md').read(),
      zip_safe=False,
      install_requires=['kombu','python-dateutil'])
