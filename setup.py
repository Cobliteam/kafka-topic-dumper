from setuptools import setup

VERSION = '0.0.2'

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
    install_requires=[
        'boto3',
        'pandas',
        'pyarrow',
        'future',
        'kafka-python==1.4.4'
    ],
    entry_points={
        'console_scripts': ['kafka-topic-dumper=kafka_topic_dumper.main:main']
    },
keywords='kakfa backup aws s3')
