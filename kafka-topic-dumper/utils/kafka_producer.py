import logging

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception

# produce asynchronously
msg = ('Message to send to kafka as a value. ' +
       'This message is the test message {:015d}')
for n in range(300000):
    producer.send(
        'test',
        msg.format(n).encode()
    ).add_errback(on_send_error)

producer.flush()
