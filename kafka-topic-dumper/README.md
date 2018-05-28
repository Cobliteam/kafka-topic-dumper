# kafka-topic-dumper
This is a simple tool to get data from a kafka topic and backup it into AWS-S3

## Output format
Backup files will be generated in parquet format. To do so kafka-topic-dumper
uses [PyArrow](https://github.com/apache/arrow/tree/master/python) package.

## Installing
Clone this repository:
```
git clone git@github.com:Cobliteam/alexstrasza-stress-test.git
```

Go to correct folder:
```
cd alexstrasza-stress-test/kafka-topic-dumper
```

Install it using setup.py file:
```
pip install -e .
```

## Configuration
To be able to upload files to AWS-S3 bucket you will need to setup your
AWS Credentials as described [here][1]. Than just export you profile like here:
```
export AWS_PROFILE=name
```

## Usage
```
$kafka-topic-dumper -h
usage: kafka-topic-dumper [-h] [-t TOPIC] [-s BOOTSTRAP_SERVERS]
                          [-n NUM_MESSAGES] [-p PATH]
                          [-m MAX_MESSAGES_PER_PACKAGE] [-d] [-b BUCKET_NAME]

Simple tool to dump kafka messages and send it to AWS S3

optional arguments:
  -h, --help            show this help message and exit
  -t TOPIC, --topic TOPIC
                        Kafka topic to fetch messages from.
  -s BOOTSTRAP_SERVERS, --bootstrap-servers BOOTSTRAP_SERVERS
                        host[:port] string (or list of host[:port] strings)
                        that the consumer should contact to bootstrap initial
                        cluster metadata. If no servers are specified, will
                        default to localhost:9092.
  -n NUM_MESSAGES, --num-messages NUM_MESSAGES
                        Number of messages to try dump.
  -p PATH, --path PATH  Path to folder where storage local dump files.
  -m MAX_MESSAGES_PER_PACKAGE, --max-messages-per-package MAX_MESSAGES_PER_PACKAGE
                        Maximum number of messages per dump file.
  -d, --dry-run         In dry run mode, kafka-topic-dumper will generate
                        local files. But will not send it to AWS S3 bucket.
  -b BUCKET_NAME, --bucket-name BUCKET_NAME
                        The AWS-S3 bucket name to send dump files.
```

## Basic example
The following command will dump to the folder named `data`,` 10000` messages
from the kafka server hosted at `localhost: 9092`. It will also send this dump
in `1000` message packets to an AWS-S3 bucket called` my-bucket`.

```
$kafka-topic-dumper -t my-topic -s localhost:9092 -n 10000 -m 1000 -p ./data \
    -b my-bucket
```

[1]: http://boto3.readthedocs.io/en/latest/guide/configuration.html#shared-credentials-file
