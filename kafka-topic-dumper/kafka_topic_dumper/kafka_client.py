import logging
import time
from os import path, remove

import boto3

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

import pandas as pd

import pyarrow as pa
from pyarrow import parquet as pq

from kafka_topic_dumper.progress_percentage import ProgressPercentage


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class KafkaClient(object):
    def __init__(self, group_id, bootstrap_servers, topic):
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = None

    def _get_consumer(self):
        if self.consumer is not None:
            return
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                key_deserializer=lambda x: b'None' if x is None else x,
                group_id=self.group_id,
                enable_auto_commit=False)
        except Exception as err:
            msg = 'Can not create KafkaConsumer instance. Reason=<{}>'
            logger.exception(msg.format(err))
            raise err

    def _close_consumer(self):
        logger.info("Closing consumer")
        self.consumer.close()
        self.consumer = None


    def _get_offsets(self):
        partitions = self.consumer.partitions_for_topic(self.topic)
        msg = "Got the following partitions=<{}> for topic=<{}>"
        logger.info(msg.format(partitions, self.topic))

        topic_partition = list(
            map(lambda p: TopicPartition(self.topic, p), partitions))
        msg = "Got the following topic partitions=<{}>"
        logger.info(msg.format(topic_partition))

        beginning_offsets = self.consumer.beginning_offsets(topic_partition)
        msg = "Got the following beginning offsets=<{}>"
        logger.info(msg.format(beginning_offsets))

        commited_offsets = {}
        msg = "Partition=<{}> has the current offset=<{}> for <{}>"
        for tp in topic_partition:
            offset = self.consumer.committed(tp)
            commited_offsets[tp] = offset
            logger.debug(msg.format(tp, offset, self.group_id))

        end_offsets = self.consumer.end_offsets(topic_partition)
        msg = "Got the following end offsets=<{}>"
        logger.info(msg.format(end_offsets))

        return beginning_offsets, commited_offsets, end_offsets

    def _calculate_offsets(self, beginning_offsets, end_offsets,
                           num_messages_to_consume):
        perfect_displacement = int(
            num_messages_to_consume / len(beginning_offsets.items()))
        offsets = {}
        num_messages_available = 0

        for tp, offset in beginning_offsets.items():
            offsets[tp] = max(beginning_offsets[tp],
                              end_offsets[tp] - perfect_displacement)
            num_messages_available += end_offsets[tp] - offsets[tp]

        return offsets, num_messages_available

    def _set_offsets(self, offsets):
        offset_and_metadata = {
                tp: OffsetAndMetadata(offset, b'') for
                tp, offset in offsets.items()}

        msg = "Generated the following offsets=<{}>"
        logger.debug(msg.format(offset_and_metadata))

        self.consumer.commit(offset_and_metadata)

    def _get_messages(self, num_messages_to_consume, dir_path, file_name):
        messages = []
        while len(messages) < num_messages_to_consume:
            record = next(self.consumer)
            line = (record.key, record.value)
            messages.append(line)

        self.consumer.commit()

        file_path = path.join(dir_path, file_name)

        df = pd.DataFrame(messages)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path, compression='gzip')

        msg = 'Got the following messages=<{}>'
        logger.debug(msg.format(messages))

    def _send_dump_file(self, dir_path, file_name, bucket_name, s3_client):
        if s3_client:
            file_path = path.join(dir_path, file_name)
            logger.info('Sending file <{}> to s3'.format(file_name))
            s3_client.upload_file(
                file_path,
                bucket_name,
                file_name,
                ExtraArgs={'ACL': 'private'},
                Callback=ProgressPercentage(file_path))
            logger.debug('Deleting file <{}>'.format(file_name))
            remove(file_path)

    def get_messages(self, num_messages_to_consume, max_package_size_in_msgs,
                     dir_path, bucket_name, dry_run):

        # set offsets
        msg = ('Will ask kafka for <{}> messages ' +
               'and save it in files with <{}> messages')
        logger.debug(msg.format(num_messages_to_consume,
                     max_package_size_in_msgs))

        self._get_consumer()
        beginning_offsets, commited_offsets, end_offsets = self._get_offsets()

        offsets, num_messages_available = self._calculate_offsets(
            beginning_offsets=beginning_offsets,
            end_offsets=end_offsets,
            num_messages_to_consume=num_messages_to_consume)

        self._set_offsets(offsets=offsets)

        # get messages
        self.consumer.subscribe(topics=[self.topic])

        msg = 'Trying to dump <{}> messages'
        logger.info(msg.format(num_messages_available))

        s3_client = None
        if not dry_run:
            s3_client = boto3.client('s3')

        remaining_messages = num_messages_available
        num_dumped_messages = 0
        timestamp = int(time.time())
        while remaining_messages > 0:
            batch_size = min(remaining_messages, max_package_size_in_msgs)
            logger.debug('Fetching batch with size=<{}>'.format(batch_size))
            file_name = (
                '{:011d}'.format(timestamp)
                + '-'
                + '{:015d}'.format(num_dumped_messages)
                + '.parquet')
            self._get_messages(
                num_messages_to_consume=batch_size,
                dir_path=dir_path,
                file_name=file_name)
            self._send_dump_file(
                dir_path=dir_path,
                file_name=file_name,
                bucket_name=bucket_name,
                s3_client=s3_client)
            remaining_messages -= batch_size
            num_dumped_messages += batch_size

        logger.info('Dump done!')

        self._close_consumer()
