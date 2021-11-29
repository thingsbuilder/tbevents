from setuptools import setup, find_packages

setup(name='tbevents',
      version='1.0.2',
      url="https://github.com/thingsbuilder/tbevents",
      license="GPL",
      classifiers=[
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: 3.7",
      ],
      author='Cassio R Eskelsen',
      author_email='eskelsen@gmail.com',
      description='This library provides functionality for an event-driven architecture using brokers like RabbitMQ '
                  'as Event Bus',
      packages=find_packages(),
      long_description=open('README.md').read(),
      install_requires=['kombu', 'python-dateutil'])
