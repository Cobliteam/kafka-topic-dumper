from kafka_topic_dumper.kafka_client import KafkaClient

bootstrap_servers = "localhost:9092"
topic = "kafka-topic-dumper-test"


class TestKafkaClient(object):

    def test__get_consumer(self):
        client = KafkaClient(bootstrap_servers=bootstrap_servers, topic=topic)

        client._get_consumer()

        assert client.consumer is not None
