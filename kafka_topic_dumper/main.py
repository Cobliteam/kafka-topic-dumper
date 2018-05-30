import argparse
import logging

from kafka_topic_dumper.kafka_client import KafkaClient


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_command_line():
    parser = argparse.ArgumentParser(
        description='Simple tool to dump kafka messages and send it to AWS S3')

    parser.add_argument('-t', '--topic', default='kafka-topic-dumper',
                        help='Kafka topic to fetch messages from.')

    parser.add_argument('-s', '--bootstrap-servers', default='localhost:9092',
                        help='host[:port] string (or list of host[:port] '
                             'strings) that the consumer should contact to '
                             'bootstrap initial cluster metadata. If no '
                             'servers are specified, will default to '
                             'localhost:9092.')

    parser.add_argument('-n', '--num-messages', default=300, type=int,
                        help='Number of messages to try dump.')

    parser.add_argument('-p', '--path', default='./data',
                        help='Path to folder where storage local dump files.')

    parser.add_argument('-m', '--max-messages-per-package', default=100,
                        type=int,
                        help='Maximum number of messages per dump file.')

    parser.add_argument('-d', '--dry-run', action='store_true',
                        help='In dry run mode, kafka-topic-dumper will '
                             'generate local files. But will not send it to '
                             'AWS S3 bucket.')

    parser.add_argument('-b', '--bucket-name', default='kafka-topic-dumper',
                        help='The AWS-S3 bucket name to send dump files.')

    parser.add_argument('-x', '--prefix', default=None,
                        help='The prefix of files to be downloaded from '
                             'AWS-S3. this option is only used in reload '
                             'mode(option -d). If no value is passed, '
                             'the program assumes that files are named in the '
                             'form TIMESTAMP-*.parquet and the gratest '
                             'timestamp will be used.')

    parser.add_argument('-l', '--reload', action='store_true',
                        help='Reload mode will download files from kafka and '
                             'send it to kafka')

    opts = parser.parse_args()

    return opts


def main():
    logging.basicConfig(
        format='%(asctime)s: %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    opts = parse_command_line()

    topic = opts.topic
    bootstrap_servers = opts.bootstrap_servers
    num_messages_to_consume = opts.num_messages
    max_messages_per_package = opts.max_messages_per_package
    dir_path_to_save_files = opts.path
    dry_run = opts.dry_run
    group_id = 'kafka_topic_dumper'
    bucket_name = opts.bucket_name
    reload_mode = opts.reload
    prefix = opts.prefix

    kafka_client = KafkaClient(
        topic=topic,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers)

    if reload_mode:
        kafka_client.reload_kafka_server(
            bucket_name=bucket_name,
            dir_path=dir_path_to_save_files,
            dump_prefix=prefix)

    else:
        kafka_client.get_messages(
            num_messages_to_consume=num_messages_to_consume,
            max_package_size_in_msgs=max_messages_per_package,
            dir_path=dir_path_to_save_files,
            bucket_name=bucket_name,
            dry_run=dry_run)
