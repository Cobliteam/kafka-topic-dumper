import logging
import time
import json
import re
from math import ceil
from os import makedirs, path, remove
from uuid import uuid4

import boto3
from time import sleep

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
    def __init__(self, bootstrap_servers, topic, group_id=None):
        if group_id is not None:
            self.group_id = group_id
            self.allow_hotreload = True
        else:
            self.group_id = 'kafka_topic_dumper_{}'.format(uuid4())
            self.allow_hotreload = False
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = None
        self.producer = None
        self.timeout = 60
        self.dump_state_topic = 'kafka-topic-dumper'
        self.s3_path = 'kafka-topic-dumper-data/'
        self.s3_client = None

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

    def _get_s3_client(self):
        if self.s3_client is None:
            self.s3_client = boto3.client('s3')
        return self.s3_client

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

        count = 0
        while not partitions and count < 500000:
            self.consumer.subscribe(topic)
            partitions = self.consumer.partitions_for_topic(topic) or []
            sleep(0.1)

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
            num_messages_to_consume / max(len(beginning_offsets), 1))
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

    def _write_messages_to_file(self, messages, local_path):
        df = pd.DataFrame(messages)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, local_path, compression='gzip')

    def _send_dump_file(self, local_path, bucket_name, dump_id):
        file_name = path.basename(local_path)
        s3_path = path.join(self.s3_path, dump_id, file_name)

        logger.info('Sending file <{}> to s3'.format(file_name))
        s3_client = self._get_s3_client()
        s3_client.upload_file(
            local_path,
            bucket_name,
            s3_path,
            ExtraArgs={'ACL': 'private'},
            Callback=ProgressPercentage(local_path))

        logger.debug('Deleting file <{}>'.format(file_name))
        remove(local_path)

    def get_messages(self, num_messages_to_consume, max_package_size_in_msgs,
                     local_dir, bucket_name, dry_run, dump_id):

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

        remaining_messages = num_messages_available
        num_dumped_messages = 0

        dump_dir = path.join(local_dir, dump_id)
        makedirs(dump_dir, exist_ok=True)
        logger.debug('Dump directory <{}> created'.format(dump_dir))

        while remaining_messages > 0:
            batch_size = min(remaining_messages, max_package_size_in_msgs)
            logger.debug('Fetching batch with size=<{}>'.format(batch_size))

            file_name = '{}-{:015d}.parquet'.format(
                dump_id, num_dumped_messages)

            local_path = path.join(local_dir, dump_id, file_name)

            messages = self._get_messages(num_messages_to_consume=batch_size)
            self._write_messages_to_file(messages=messages,
                                         local_path=local_path)
            if not dry_run:
                self._send_dump_file(local_path=local_path,
                                     bucket_name=bucket_name,
                                     dump_id=dump_id)
            remaining_messages -= batch_size
            num_dumped_messages += batch_size

        logger.info('Dump done!')

    def find_latest_dump_id(self, bucket_name):
        paginator = self._get_s3_client().get_paginator('list_objects_v2')

        prefix = self.s3_path.rstrip('/') + '/'

        response_iterator = paginator.paginate(Bucket=bucket_name,
                                               Prefix=prefix,
                                               Delimiter='/')
        def strip(r):
            return r['Prefix'][len(prefix):].rstrip('/')

        prefixes = []
        for response in response_iterator:
            prefixes.extend(map(strip, response['CommonPrefixes']))

        dump_id = max(prefixes)
        logger.debug('Prefix chosen was <{}>'.format(dump_id))

        return dump_id

    def _get_file_names(self, bucket_name, dump_id):
        paginator = self._get_s3_client().get_paginator('list_objects_v2')
        dump_path = path.join(self.s3_path, dump_id) + '/'

        response_iterator = paginator.paginate(Bucket=bucket_name,
                                               Prefix=dump_path)
        file_names = []
        for response in response_iterator:
            if response['KeyCount'] > 0:
                file_names.extend((f['Key'], f['Size'])
                                  for f in response['Contents'])
        file_names.sort()

        if not file_names:
            msg = 'Can not found files for this dump id <{}>'
            logger.error(msg.format(dump_id))
            raise Exception('EmptyS3Response')

        return file_names

    def _gen_state(self, dump_id):
        _, _, end_offsets = self._get_offsets()

        if not end_offsets:
            msg = 'Can not find offsets for topic <{}>'
            raise Exception(msg.format(self.topic))

        state_offsets = {}

        for partition, offset in end_offsets.items():
            state_offsets[partition.partition] = offset

        state = {
            'dump_id': dump_id,
            'topic_name': self.topic,
            'offsets': state_offsets,
            'dump_date': int(time.time())}

        return state

    def _save_state(self, state):
        future = self.producer.send(
            topic=self.dump_state_topic,
            key=self.topic,
            value=json.dumps(state))
        future.get(timeout=self.timeout)
        logger.info('State saved')

    def _get_last_state_message(self, dump_id):
        beginning_offsets, _, end_offsets = (
            self._get_offsets(topic=self.dump_state_topic))

        if beginning_offsets:
            offsets, num_messages_available = self._calculate_offsets(
                beginning_offsets=beginning_offsets,
                end_offsets=end_offsets,
                num_messages_to_consume=1)
            self._set_offsets(offsets)
            self.consumer.subscribe(self.dump_state_topic)
            messages = [json.loads(m.decode()) for _, m in
                        self._get_messages(num_messages_available)]
            if messages:
                last_state_message = max(messages,
                                         key=lambda m: m['dump_date'])
                return last_state_message

        return None

    def _get_state(self, dump_id):
        if self.allow_hotreload:
            state_message = self._get_last_state_message(dump_id)
            if state_message and \
               state_message['topic_name'] == self.topic and \
               state_message['dump_id'] == dump_id:
                    return state_message['offsets']
        return None

    def _reset_offsets(self, dump_offsets):
        logger.info('Messages already uploaded. Just resetting offsets')
        partitions = self._get_partitions(self.topic)
        offsets = {}

        for partition in partitions:
            offsets[partition] = dump_offsets[str(partition.partition)]

        logger.debug('Will reset offsets to <{}>'.format(offsets))

        self._set_offsets(offsets)

    def _load_dump(self, bucket_name, dump_id, download_dir, files,
                   transformer_class):
        s3_client = self._get_s3_client()

        state = self._gen_state(dump_id)

        current_file_number = 0
        msg = "Loading messages from file {}/{} to kafka"
        for file_name, file_size in files:
            current_file_number += 1
            tmp_name = '{}.tmp'.format(path.basename(file_name))
            file_path = path.join(download_dir, tmp_name)
            s3_client.download_file(
                Bucket=bucket_name,
                Filename=file_path,
                Key=file_name,
                Callback=ProgressPercentage(tmp_name, file_size))
            logger.info(msg.format(current_file_number, len(files)))
            try:
                table = pq.read_table(file_path)
                df = table.to_pandas()
                for raw_row in df.itertuples():
                    for row in transformer_class.transform(raw_row):
                        self.producer.send(self.topic, key=row[1],
                                           value=row[2])
                logger.debug('File <{}> reloaded to kafka'.format(file_path))
                self.producer.flush()
            finally:
                remove(file_path)

        self._save_state(state)

    def reload_kafka_server(self, bucket_name, local_dir, dump_id,
                            transformer_class):
        dump_offsets = self._get_state(dump_id)

        if dump_offsets:
            self._reset_offsets(dump_offsets=dump_offsets)
        else:
            files = self._get_file_names(bucket_name=bucket_name,
                                         dump_id=dump_id)
            self._load_dump(bucket_name=bucket_name, dump_id=dump_id,
                            download_dir=local_dir, files=files,
                            transformer_class=transformer_class)

        logger.info('Reload done!')

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
