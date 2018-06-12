import argparse
import logging
import tempfile
import sys

from kafka_topic_dumper.kafka_client import KafkaClient


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_command_line():
    parser = argparse.ArgumentParser(description='Simple tool to dump kafka '
                                                 'messages and send it to AWS '
                                                 'S3')

    parser.add_argument('-t', '--topic', default='test',
                        help='Kafka topic to fetch messages from.')

    parser.add_argument('-s', '--bootstrap-servers', default='localhost:9092',
                        help='host[:port] string (or list of host[:port] '
                             'strings) that the consumer should contact to '
                             'bootstrap initial cluster metadata. If no '
                             'servers are specified, will default to '
                             'localhost:9092.')

    parser.add_argument('-b', '--bucket-name', default='kafka-topic-dumper',
                        help='The AWS-S3 bucket name to send dump files.')

    parser.add_argument('-p', '--path', default=None,
                        help='Path to folder where to store local files.')


    cmds = parser.add_subparsers(help='sub-command help')

    dump_cmd = cmds.add_parser('dump',
                               help='Dump mode will fetch messages from kafka '
                                    'cluster and send then to AWS-S3.')

    dump_cmd.add_argument('-n', '--num-messages', default=300, type=int,
                          help='Number of messages to try dump.')

    dump_cmd.add_argument('-m', '--max-messages-per-package', default=100,
                          type=int,
                          help='Maximum number of messages per dump file.')

    dump_cmd.add_argument('-d', '--dry-run', action='store_true',
                          help='In dry run mode, kafka-topic-dumper will '
                               'generate local files. But will not send it to '
                               'AWS S3 bucket.')

    dump_cmd.set_defaults(action='dump')

    reload_cmd = cmds.add_parser('reload',
                                 help='Reload mode will download files from '
                                      'AWS-S3 and send then to kafka.')

    reload_cmd.add_argument('-x', '--prefix', default=None,
                            help='The prefix of files to be downloaded from '
                                 'AWS-S3. this option is only used in reload '
                                 'mode(option -d). If no value is passed, the '
                                 'program assumes that files are named in the '
                                 'form TIMESTAMP-*.parquet and the gratest '
                                 'timestamp will be used.')

    reload_cmd.set_defaults(action='reload')

    opts = parser.parse_args()

    if getattr(opts, 'action', None) is None:
        parser.print_help()
        sys.exit(1)

    print(opts)

    return opts


def main():
    logging.basicConfig(
        format='%(asctime)s: %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    opts = parse_command_line()

    bootstrap_servers = opts.bootstrap_servers
    bucket_name = opts.bucket_name
    group_id = 'kafka_topic_dumper'
    topic = opts.topic

    with KafkaClient(topic=topic, group_id=group_id,
                     bootstrap_servers=bootstrap_servers) as kafka_client:
        with tempfile.TemporaryDirectory() as tmp_dir:
            dir_path = opts.path or tmp_dir

            if opts.action == 'dump':
                kafka_client.get_messages(
                    num_messages_to_consume=opts.num_messages,
                    max_package_size_in_msgs=opts.max_messages_per_package,
                    dir_path=dir_path,
                    bucket_name=bucket_name,
                    dry_run=opts.dry_run)

            elif opts.action == 'reload':
                prefix = opts.prefix
                kafka_client.reload_kafka_server(
                    bucket_name=bucket_name,
                    dir_path=dir_path,
                    dump_prefix=prefix)
