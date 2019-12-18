import os

from setuptools import setup

VERSION = '0.0.2'

def get_requirements():
    """Gets the minimum libraries needed for installation and usage
    """
    base_path = os.path.dirname(__file__)
    with open(os.path.join(base_path, 'requirements.txt')) as reqs:
        return [req.strip() for req in reqs]


setup(
    name='kafka-topic-dumper',
    packages=['kafka_topic_dumper'],
    version=VERSION,
    description='Dump messages from a kafka topic and send it to Amazon S3',
    url='https://github.com/Cobliteam/kafka-topic-dump',
    download_url='https://github.com/Cobliteam/kafka-topic-dump/archive/{}.tar.gz'.format(VERSION),
    author='Evandro Sanches',
    author_email='evnadro@cobli.co',
    license='MIT',
    install_requires=get_requirements(),
    entry_points={
        'console_scripts': ['kafka-topic-dumper=kafka_topic_dumper.main:main']
    },
keywords='kakfa backup aws s3')
