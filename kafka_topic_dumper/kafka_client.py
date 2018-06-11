import logging
import time
import json
import re
from math import ceil
from os import path, remove

import boto3

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata, TopicPartition

import pandas as pd

import pyarrow as pa
from pyarrow import parquet as pq

from kafka_topic_dumper.progress_percentage import ProgressPercentage


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def bytes_serializer(value):
    if value is None:
        return
    if type(value) is bytes:
        return value
    return str.encode(value)


class KafkaClient(object):
    def __init__(self, group_id, bootstrap_servers, topic):
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = None
        self.producer = None
        self.timeout = 1
        self.dump_state_topic = 'kafka-topic-dumper'
        self.s3_path = 'kafka-topic-dumper-data/'

    def _get_consumer(self):
        if self.consumer is not None:
            return
        try:
            logger.info('Starting consumer')
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                enable_auto_commit=False)
        except Exception as err:
            msg = 'Can not create KafkaConsumer instance. Reason=<{}>'
            logger.exception(msg.format(err))
            raise err

    def _get_producer(self):
        if self.producer is not None:
            return
        try:
            logger.info('Starting producer')
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                key_serializer=bytes_serializer,
                value_serializer=bytes_serializer)
        except Exception as err:
            msg = 'Can not create KafkaProducer instance. Reason=<{}>'
            logger.exception(msg.format(err))
            raise err

    def open(self):
        self._get_consumer()
        self._get_producer()

    def _close_consumer(self):
        logger.info("Closing consumer")
        self.consumer.close()
        self.consumer = None

    def _close_producer(self):
        logger.info("Closing producer")
        self.producer.flush()
        logger.debug('Statistics {}'.format(self.producer.metrics()))
        self.producer.close()
        self.producer = None

    def close(self):
        self._close_consumer()
        self._close_producer()

    def _get_partitions(self, topic):
        partitions = self.consumer.partitions_for_topic(topic) or []
        msg = "Got the following partitions=<{}> for topic=<{}>"
        logger.info(msg.format(partitions, topic))

        topic_partitions = list(
            map(lambda p: TopicPartition(topic, p), partitions))
        msg = "Got the following topic partitions=<{}>"
        logger.info(msg.format(topic_partitions))
        return topic_partitions

    def _get_offsets(self, topic=None):
        if topic is None:
            topic = self.topic
        topic_partitions = self._get_partitions(topic=topic)
        beginning_offsets = (
            self.consumer.beginning_offsets(topic_partitions) or {})
        msg = "Got the following beginning offsets=<{}>"
        logger.info(msg.format(beginning_offsets))

        commited_offsets = {}
        msg = "Partition=<{}> has the current offset=<{}> for <{}>"
        for tp in topic_partitions:
            offset = self.consumer.committed(tp)
            commited_offsets[tp] = offset
            logger.debug(msg.format(tp, offset, self.group_id))

        end_offsets = self.consumer.end_offsets(topic_partitions) or {}
        msg = "Got the following end offsets=<{}>"
        logger.info(msg.format(end_offsets))

        return beginning_offsets, commited_offsets, end_offsets

    def _calculate_offsets(self, beginning_offsets, end_offsets,
                           num_messages_to_consume):
        perfect_displacement = ceil(
            num_messages_to_consume / len(beginning_offsets))
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

    def _get_messages(self, num_messages_to_consume):
        messages = []
        while len(messages) < num_messages_to_consume:
            record = next(self.consumer)
            line = (record.key, record.value)
            messages.append(line)
        self.consumer.commit()

        return messages

    def _write_to_file(self, messages, dir_path, file_name):
        file_path = path.join(dir_path, file_name)

        df = pd.DataFrame(messages)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path, compression='gzip')

    def _send_dump_file(self, dir_path, file_name, bucket_name, s3_client):
        if s3_client:
            file_path = path.join(dir_path, file_name)
            file_name = self.s3_path + file_name
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

        beginning_offsets, commited_offsets, end_offsets = self._get_offsets()

        offsets, num_messages_available = self._calculate_offsets(
            beginning_offsets=beginning_offsets,
            end_offsets=end_offsets,
            num_messages_to_consume=num_messages_to_consume)

        self._set_offsets(offsets)

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
        file_name_base = '{:011d}-{:015d}.parquet'
        while remaining_messages > 0:
            batch_size = min(remaining_messages, max_package_size_in_msgs)
            logger.debug('Fetching batch with size=<{}>'.format(batch_size))
            file_name = file_name_base.format(timestamp, num_dumped_messages)
            messages = self._get_messages(num_messages_to_consume=batch_size)
            self._write_to_file(messages=messages, dir_path=dir_path,
                                file_name=file_name)
            self._send_dump_file(dir_path=dir_path, file_name=file_name,
                                 bucket_name=bucket_name, s3_client=s3_client)
            remaining_messages -= batch_size
            num_dumped_messages += batch_size

        logger.info('Dump done!')

    def _get_file_names(self, bucket_name, dump_prefix, s3_client):
        paginator = s3_client.get_paginator('list_objects_v2')

        if dump_prefix is None:
            prefix = self.s3_path
            sufix = '-'
            reg = re.compile('^{}(.*){}$'.format(prefix, sufix))

            response_iterator = paginator.paginate(Bucket=bucket_name,
                                                   Prefix=prefix,
                                                   Delimiter='-')
            def striper(string):
                match = reg.match(string['Prefix'])
                if match is not None:
                    return match.group(1)

            prefixes = []
            for response in response_iterator:
                response_prefixes = filter(
                    lambda x: x is not None,
                    map(striper, response['CommonPrefixes']))
                prefixes += response_prefixes

            dump_prefix = max(prefixes)
            logger.info('Prefix chosen was <{}>'.format(dump_prefix))

        dump_path = prefix + dump_prefix

        response_iterator = paginator.paginate(Bucket=bucket_name,
                                               Prefix=dump_path)
        file_names = []
        for response in response_iterator:
            if response['KeyCount'] > 0:
                file_names = [(f['Key'], f['Size'])
                              for f in response['Contents']]
        file_names.sort()
        return dump_prefix, file_names

    def _save_state(self, test_id):
        _, _, end_offsets = self._get_offsets()

        if not end_offsets:
            msg = 'Can not find offsets for topic <{}>'
            raise Exception(msg.format(self.topic))

        test_offsets = {}

        for partition, offset in end_offsets.items():
            test_offsets[partition.partition] = offset

        test_state = {
            'test_id': test_id,
            'topic_name': self.topic,
            'offsets': test_offsets}

        future = self.producer.send(
            topic=self.dump_state_topic,
            key=test_id,
            value=json.dumps(test_state))
        future.get(timeout=self.timeout)
        logger.info('State saved')

    def _get_state(self, test_id):
        beginning_offsets, _, end_offsets = (
            self._get_offsets(topic=self.dump_state_topic))
        if beginning_offsets:
            offsets, num_messages_available = self._calculate_offsets(
                beginning_offsets=beginning_offsets,
                end_offsets=end_offsets,
                num_messages_to_consume=1)
            self._set_offsets(offsets)
            self.consumer.subscribe(self.dump_state_topic)
            for message in self._get_messages(num_messages_available):
                state = json.loads(message[1].decode())
                if  state.get('topic_name', '') == self.topic and \
                    state.get('test_id', '') == test_id:
                    return state.get('offsets', None)
            return None

    def reload_kafka_server(self, bucket_name, dir_path, dump_prefix=None):

        s3_client = boto3.client('s3')

        test_id, file_names = self._get_file_names(
            bucket_name=bucket_name,
            dump_prefix=dump_prefix,
            s3_client=s3_client)

        dump_offsets = self._get_state(test_id)

        if dump_offsets is None:
            self._save_state(test_id)

            for file_name, file_size in file_names:
                file_path = path.join(
                    dir_path, '{}.tmp'.format(path.basename(file_name)))
                s3_client.download_file(
                    Bucket=bucket_name,
                    Filename=file_path,
                    Key=file_name,
                    Callback=ProgressPercentage(
                        '{}.tmp'.format(file_name),
                        file_size))
                table = pq.read_table(file_path)
                df = table.to_pandas()
                for row in df.itertuples():
                    future = self.producer.send(self.topic, key=row[1],
                                                value=row[2])
                    future.get(timeout=self.timeout)
                logger.debug('File <{}> reloaded to kafka'.format(file_path))
                remove(file_path)
        else:
            logger.info('Messages already uploaded. Just resetting offsets')
            partitions = self._get_partitions(self.topic)
            offsets = {}
            for partition in partitions:
                offsets[partition] = dump_offsets[str(partition.partition)]
            self._set_offsets(offsets)
            logger.debug('Should reset offsets to {}'.format(offsets))

        logger.info('Reload done!')

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
