import logging

from kafka_topic_dumper.kafka_client import KafkaClient


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    logging.basicConfig(
        format='%(name)s - %(levelname)s - %(message)s')

    topic = 'test'
    bootstrap_servers = 'localhost:9092'
    group_id = 'kafka_topic_dumper'
    num_messages_to_consume = 300  # 000
    max_package_size_in_msgs = 50  # 000
    dir_path_to_save_files = 'data'
    bucket_name = 'cobli-alexstrasza-stress-test'
    dry_run = False

    kafka_client = KafkaClient(
        topic=topic,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers)

    kafka_client.get_messages(
        num_messages_to_consume=num_messages_to_consume,
        max_package_size_in_msgs=max_package_size_in_msgs,
        dir_path=dir_path_to_save_files,
        bucket_name=bucket_name,
        dry_run=dry_run)
